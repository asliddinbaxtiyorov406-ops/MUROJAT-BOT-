import asyncio
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache
from time import monotonic
from urllib.parse import parse_qsl, quote_plus, urlencode, urlparse

from aiogram import BaseMiddleware, Bot, Dispatcher
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
import dns.resolver
from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument, UpdateOne


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
ADMIN_NAME = (os.getenv("ADMIN_NAME") or "Botirali Turdiyev").strip()
ADMIN_URL = (os.getenv("ADMIN_URL") or "").strip()
REQUIRED_CHANNEL = (os.getenv("REQUIRED_CHANNEL") or "").strip()
CHANNEL_ID = (os.getenv("CHANNEL_ID") or "").strip()
MONGODB_URI = (os.getenv("MONGODB_URI") or "mongodb://localhost:27017").strip()
MONGODB_DB_NAME = (os.getenv("MONGODB_DB_NAME") or "murojat_bot").strip()

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN .env faylda topilmadi")

if ADMIN_ID and not ADMIN_ID.isdigit():
    raise ValueError("ADMIN_ID raqam bo'lishi kerak")

if not REQUIRED_CHANNEL:
    raise ValueError("REQUIRED_CHANNEL .env faylda topilmadi")

if not MONGODB_DB_NAME:
    raise ValueError("MONGODB_DB_NAME bo'sh bo'lmasligi kerak")

ADMIN_USER_ID = int(ADMIN_ID) if ADMIN_ID else None


dp = Dispatcher()
LEGACY_SQLITE_PATH = "murojat_bot.db"
mongo_client: MongoClient | None = None
mongo_db = None
users_collection = None
requests_collection = None
counters_collection = None
meta_collection = None
DB_BACKEND = "mongo"
SQLITE_BUSY_TIMEOUT_MS = 5000
SQLITE_CONNECT_TIMEOUT_SECONDS = 10
SUBSCRIPTION_CACHE_TTL_TRUE_SECONDS = 300
SUBSCRIPTION_CACHE_TTL_FALSE_SECONDS = 15
MISSING_USER = object()
USER_CACHE: dict[int, dict[str, object] | object] = {}
SUBSCRIPTION_CACHE: dict[int, tuple[bool, float]] = {}


def _resolve_cname_target(hostname: str) -> str | None:
    current = hostname.rstrip(".")
    try:
        answers = dns.resolver.resolve(current, "CNAME")
    except (
        dns.resolver.NoAnswer,
        dns.resolver.NXDOMAIN,
        dns.resolver.NoNameservers,
        dns.resolver.LifetimeTimeout,
    ):
        return None

    target = str(answers[0].target).rstrip(".")
    if not target or target == current:
        return None
    return target


@lru_cache(maxsize=4)
def build_mongo_sni_alias_map(mongo_uri: str) -> dict[str, str]:
    if not mongo_uri.startswith("mongodb+srv://"):
        return {}

    cluster_host = urlparse(mongo_uri).hostname
    if not cluster_host:
        return {}

    record_name = f"_mongodb._tcp.{cluster_host}"
    try:
        srv_answers = dns.resolver.resolve(record_name, "SRV")
    except (
        dns.resolver.NoAnswer,
        dns.resolver.NXDOMAIN,
        dns.resolver.NoNameservers,
        dns.resolver.LifetimeTimeout,
    ):
        return {}

    alias_map: dict[str, str] = {}
    for record in srv_answers:
        alias_host = str(record.target).rstrip(".")
        canonical_host = _resolve_cname_target(alias_host)
        if canonical_host and canonical_host != alias_host:
            alias_map[alias_host] = canonical_host

    return alias_map


@lru_cache(maxsize=4)
def _load_mongo_srv_txt_options(cluster_host: str) -> dict[str, str]:
    try:
        txt_answers = dns.resolver.resolve(cluster_host, "TXT")
    except (
        dns.resolver.NoAnswer,
        dns.resolver.NXDOMAIN,
        dns.resolver.NoNameservers,
        dns.resolver.LifetimeTimeout,
    ):
        return {}

    options: dict[str, str] = {}
    for record in txt_answers:
        text = "".join(
            part.decode() if isinstance(part, bytes) else str(part)
            for part in getattr(record, "strings", ())
        )
        if not text:
            text = str(record).replace('"', "")
        for chunk in text.split("&"):
            if "=" not in chunk:
                continue
            key, value = chunk.split("=", 1)
            if key and value:
                options[key] = value
    return options


@lru_cache(maxsize=4)
def discover_mongo_replica_set_name(mongo_uri: str) -> str | None:
    alias_map = build_mongo_sni_alias_map(mongo_uri)
    if not alias_map:
        return None

    canonical_host = next(iter(alias_map.values()), None)
    if not canonical_host:
        return None

    client = None
    try:
        client = MongoClient(
            f"mongodb://{canonical_host}/?tls=true&directConnection=true",
            serverSelectionTimeoutMS=3000,
        )
        hello_response = client.admin.command("hello")
        set_name = hello_response.get("setName")
        return str(set_name).strip() if set_name else None
    except Exception as error:
        logging.warning("MongoDB replicaSet nomini aniqlab bo'lmadi: %s", error)
        return None
    finally:
        if client is not None:
            client.close()


@lru_cache(maxsize=4)
def build_effective_mongo_uri(mongo_uri: str) -> str:
    if not mongo_uri.startswith("mongodb+srv://"):
        return mongo_uri

    parsed = urlparse(mongo_uri)
    cluster_host = parsed.hostname
    alias_map = build_mongo_sni_alias_map(mongo_uri)
    if not cluster_host or not alias_map:
        return mongo_uri

    query_options = _load_mongo_srv_txt_options(cluster_host)
    query_options.update(dict(parse_qsl(parsed.query, keep_blank_values=True)))
    query_options["tls"] = "true"

    replica_set_name = discover_mongo_replica_set_name(mongo_uri)
    if replica_set_name:
        query_options["replicaSet"] = replica_set_name

    credentials = ""
    if parsed.username is not None:
        credentials = quote_plus(parsed.username)
        if parsed.password is not None:
            credentials = f"{credentials}:{quote_plus(parsed.password)}"
        credentials = f"{credentials}@"

    canonical_hosts = ",".join(dict.fromkeys(alias_map.values()))
    path = parsed.path or "/"
    query = urlencode(query_options)
    effective_uri = f"mongodb://{credentials}{canonical_hosts}{path}"
    if query:
        effective_uri = f"{effective_uri}?{query}"
    return effective_uri


class SubscriptionRequiredMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if not user or is_staff_user(user.id):
            return await handler(event, data)

        if isinstance(event, CallbackQuery) and event.data == "check_subscription":
            return await handler(event, data)

        bot = data.get("bot")
        state = data.get("state")
        if bot is None or state is None:
            return await handler(event, data)

        if await is_user_subscribed(bot, user.id, force_refresh=True):
            return await handler(event, data)

        if isinstance(event, CallbackQuery):
            await event.answer(localize_text_for_user(user.id, "Avval kanalga a'zo bo'ling"), show_alert=True)
            if event.message:
                await send_subscription_prompt(event.message, state, user_id=user.id)
            return None

        await send_subscription_prompt(event, state, user_id=user.id)
        return None


dp.message.outer_middleware(SubscriptionRequiredMiddleware())
dp.callback_query.outer_middleware(SubscriptionRequiredMiddleware())


class AppealState(StatesGroup):
    waiting_for_attachment_choice = State()
    waiting_for_attachment = State()
    waiting_for_section_text = State()


class AdminReplyState(StatesGroup):
    waiting_for_reply = State()


class RegisterState(StatesGroup):
    waiting_for_full_name = State()
    waiting_for_phone = State()

QAMASHI_HUDUDLARI = [
    "Azlartepa",
    "Badaxshon",
    "Balandchayla",
    "Berdoli",
    "Boburtepa",
    "Bog'obod",
    "Boybo'ri",
    "Bunyodkor",
    "Changak",
    "Chim",
    "Chuqurqishloq",
    "Do'stlik",
    "Do'vud",
    "Elobod",
    "G'ishtli",
    "Guliston",
    "Gulshan",
    "Ibn Sino",
    "Jonbuz",
    "Qamay",
    "Kaptarxona",
    "Katta O'ra",
    "Qishlik",
    "Qiziltepa",
    "Ko'kbuloq",
    "Loyqasoy",
    "Mang'it",
    "Mayda",
    "Mehr",
    "Navoiy",
    "Nurli yo'l",
    "Odoqjonbuz",
    "O'lg'ubek",
    "Olmazor",
    "Oqg'uzar",
    "Oqrabod",
    "O'rtadara",
    "Oynako'l",
    "O'zbekiston",
    "Paxtaobod",
    "Qizilqishloq",
    "Qorabog'",
    "Qoratepa",
    "Qorasuv",
    "Qo'ng'irot",
    "Quyiyangi",
    "Rabod",
    "Samarqand",
    "Sarbozor",
    "Sohibkor",
    "Tinchlik",
    "To'qboy",
    "Uzun",
    "Yangi Avlod",
    "Yortepa",
    "Yuksalish",
]

REGION_CODES = {str(index): region for index, region in enumerate(QAMASHI_HUDUDLARI, start=1)}
REGION_NAME_LOOKUP = {
    re.sub(r"\s+", " ", region).strip().casefold(): region
    for region in QAMASHI_HUDUDLARI
}


def normalize_region_name(region_name: str | None) -> str:
    return re.sub(r"\s+", " ", str(region_name or "")).strip().casefold()


def resolve_region_name(region_name: str | None) -> str | None:
    if not region_name:
        return None

    return REGION_NAME_LOOKUP.get(normalize_region_name(region_name))


def build_region_admin_env_key(region: str) -> str:
    value = str(region).upper()
    for char in ("'", "`"):
        value = value.replace(char, "")
    value = value.replace("-", "_").replace(" ", "_")
    value = re.sub(r"[^A-Z0-9_]", "", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return f"MFY_ADMIN_{value}"


def validate_region_leader_ids(
    mappings: dict[str, int],
    missing_entries: list[str],
) -> dict[str, int]:
    if not mappings:
        raise ValueError(
            "Har bir MFY uchun alohida admin majburiy. "
            "`.env` ichida barcha `MFY_ADMIN_*` qatorlarini kiriting."
        )

    missing_regions = [region for region in QAMASHI_HUDUDLARI if region not in mappings]
    leader_regions: dict[int, list[str]] = {}
    for region, user_id in mappings.items():
        leader_regions.setdefault(user_id, []).append(region)

    duplicate_assignments = {
        user_id: regions for user_id, regions in leader_regions.items() if len(regions) > 1
    }

    if missing_regions or duplicate_assignments:
        errors: list[str] = []
        if missing_entries:
            errors.append("To'ldirilmagan .env qatorlari: " + "; ".join(missing_entries))
        if missing_regions:
            errors.append("Biriktirilmagan MFYlar: " + ", ".join(missing_regions))
        if duplicate_assignments:
            duplicate_text = "; ".join(
                f"{user_id} -> {', '.join(regions)}"
                for user_id, regions in sorted(duplicate_assignments.items())
            )
            errors.append("Bir admin bir nechta MFYga biriktirilgan: " + duplicate_text)

        raise ValueError(
            "Har bir MFY uchun bittadan alohida admin biriktirilishi kerak.\n" + "\n".join(errors)
        )

    return mappings


def load_region_leader_configs() -> tuple[dict[str, int], dict[str, str]]:
    mappings: dict[str, int] = {}
    contact_urls: dict[str, str] = {}
    missing_entries: list[str] = []

    for region in QAMASHI_HUDUDLARI:
        env_key = build_region_admin_env_key(region)
        raw_value = (os.getenv(env_key) or "").strip()
        if not raw_value:
            missing_entries.append(f"{region} -> {env_key}")
            continue

        match = re.fullmatch(r"(\d+)(?:\s*\|\s*(\S+))?", raw_value)
        if not match:
            raise ValueError(
                f"{env_key} noto'g'ri formatda. Misol: "
                f"{env_key}=123456789 | @"
                f"{env_key.removeprefix('MFY_ADMIN_').lower()}_admin"
            )

        raw_user_id, raw_contact_url = match.groups()
        mappings[region] = int(raw_user_id)
        contact_urls[region] = (raw_contact_url or "").strip()

    return validate_region_leader_ids(mappings, missing_entries), contact_urls


REGION_LEADER_IDS, REGION_LEADER_URLS = load_region_leader_configs()
REGION_ADMIN_USER_IDS = frozenset(REGION_LEADER_IDS.values())
PRIMARY_ADMIN_USER_IDS = frozenset(
    ({ADMIN_USER_ID} if ADMIN_USER_ID is not None else set()) | REGION_ADMIN_USER_IDS
)
OPTIONAL_ATTACHMENT_SECTION_KEYS = frozenset(
    {
        "\U0001F4E9 Murojaat yuborish",
        "\U0001F4A1 Taklif berish",
        "\U0001F4B0 Moliyaviy yordam",
    }
)
YOUTH_OPPORTUNITIES_SECTION_KEY = "\U0001F31F Yoshlar imkoniyatlari"
YOUTH_OPPORTUNITY_CONFIG = {
    "\U0001F4DA Ta'lim": {
        "text": (
            "\U0001F4DA Ta'lim imkoniyatlari\n\n"
            "Bu yo'nalishda quyidagilarga e'tibor bering:\n"
            "1. Oliy ta'lim, grant va stipendiya dasturlarini muntazam kuzatib boring.\n"
            "2. Chet tili, IT va kasbiy kurslarda sertifikat yig'ing.\n"
            "3. Hujjatlaringizni tayyor saqlang: pasport, attestat yoki diplom, CV va sertifikatlar.\n"
            "4. Bepul va arzon onlayn kurslardan foydalanib portfolio tuzing.\n"
            "5. O'zingizga mos yo'nalish uchun aniq oylik reja tuzing.\n\n"
            "Yana boshqa yo'nalishni ko'rish uchun pastdagi tugmalardan birini tanlang."
        )
    },
    "\U0001F4BC Ish": {
        "text": (
            "\U0001F4BC Ish imkoniyatlari\n\n"
            "Ish topishda sizga foydali bo'ladigan tavsiyalar:\n"
            "1. Rezyume tayyorlang va unda ko'nikma, tajriba, o'qish joyi hamda aloqa ma'lumotlarini yozing.\n"
            "2. Amaliyot, yarim stavka ish va masofaviy vakansiyalarni ham ko'rib chiqing.\n"
            "3. Kompyuter savodxonligi, muloqot va til ko'nikmalarini kuchaytiring.\n"
            "4. Suhbat uchun o'zingiz haqingizda qisqa va aniq taqdimot tayyorlab qo'ying.\n"
            "5. Halol va rasmiy ish o'rinlarini tanlang, shartlarni oldindan tekshiring.\n\n"
            "Yana boshqa yo'nalishni ko'rish uchun pastdagi tugmalardan birini tanlang."
        )
    },
    "\U0001F680 Startap": {
        "text": (
            "\U0001F680 Startap yo'nalishi\n\n"
            "Startap boshlash uchun quyidagi qadamlar muhim:\n"
            "1. Avval muammoni aniq tanlang va yechimingiz kimga kerakligini tekshiring.\n"
            "2. Kichik jamoa tuzing: texnik, marketing yoki tashkiliy sherik foydali bo'ladi.\n"
            "3. Katta loyiha kutmasdan, minimal mahsulot yoki xizmat bilan boshlang.\n"
            "4. Xarajat, daromad va mijoz segmenti bo'yicha sodda biznes reja tuzing.\n"
            "5. Pitch, taqdimot va tanlovlar orqali hamkor yoki investitsiya izlang.\n\n"
            "Yana boshqa yo'nalishni ko'rish uchun pastdagi tugmalardan birini tanlang."
        )
    },
    "\U0001F30D Tanlovlar": {
        "text": (
            "\U0001F30D Tanlovlar haqida\n\n"
            "Tanlovlarda muvaffaqiyatli qatnashish uchun:\n"
            "1. Nizom, yosh cheklovi va topshirish muddatlarini diqqat bilan o'qing.\n"
            "2. Portfolio, esse, sertifikat, loyiha yoki tavsiyanoma kerak bo'lishi mumkin.\n"
            "3. Hujjatlarni oldindan tayyorlang va topshirishni oxirgi kunga qoldirmang.\n"
            "4. Tuman, viloyat, respublika va xalqaro tanlovlarni muntazam kuzatib boring.\n"
            "5. Ariza topshirgandan keyin email va telefon orqali keladigan xabarlarni tekshirib boring.\n\n"
            "Yana boshqa yo'nalishni ko'rish uchun pastdagi tugmalardan birini tanlang."
        )
    },
    "\U0001F9E0 Rivojlanish": {
        "text": (
            "\U0001F9E0 Shaxsiy rivojlanish\n\n"
            "O'zingizni rivojlantirish uchun foydali odatlar:\n"
            "1. Har kuni kamida 30-60 daqiqa yangi ko'nikmaga vaqt ajrating.\n"
            "2. Chet tili, IT, ommaviy nutq, liderlik va moliyaviy savodxonlikni kuchaytiring.\n"
            "3. Kitob o'qish, mentor topish va volontyorlik tajribangizni oshiradi.\n"
            "4. Maqsadlaringizni 3 oy, 6 oy va 1 yil bo'yicha yozib chiqing.\n"
            "5. Har oy natijangizni baholab, rejangizni yangilang.\n\n"
            "Yana boshqa yo'nalishni ko'rish uchun pastdagi tugmalardan birini tanlang."
        )
    },
}

SECTION_CONFIG = {
    "\U0001F4E9 Murojaat yuborish": {
        "title": "Murojaat",
        "prompt": (
            "\U0001F4E9 Murojaat yuborish bo'limi\n\n"
            "Murojaatingizni bitta xabarda aniq va tushunarli qilib yozing.\n\n"
            "Quyidagilarni kiritsangiz yaxshi bo'ladi:\n"
            "1. Muammo yoki murojaat mazmuni\n"
            "2. Manzil yoki joy nomi\n"
            "3. Bog'lanish uchun telefon raqam\n\n"
            "Xabaringizni hozir yuboring \U0001F447"
        ),
        "success": (
            "Murojaatingiz qabul qilindi. \U00002705\n\n"
            "Ma'lumotingiz Yoshlar yetakchisiga yuborildi. "
            "Mas'ullar murojaatingizni ko'rib chiqadi."
        ),
    },
    "\U0001F4A1 Taklif berish": {
        "title": "Taklif",
        "prompt": (
            "\U0001F4A1 Taklif berish bo'limi\n\n"
            "Mahalla, yoshlar, ta'lim, tadbir yoki boshqa yo'nalishlar bo'yicha "
            "taklifingizni yozib yuboring.\n\n"
            "Quyidagilarni kiriting:\n"
            "1. Taklif mazmuni\n"
            "2. Kimlar uchun foydali bo'lishi\n"
            "3. Amalga oshirish bo'yicha qisqa fikringiz\n\n"
            "Taklifingizni hozir yuboring \U0001F447"
        ),
        "success": (
            "Taklifingiz qabul qilindi. \U00002705\n\n"
            "Tashabbusingiz Yoshlar yetakchisiga yuborildi. "
            "Fikrlaringiz albatta ko'rib chiqiladi."
        ),
    },
    "\U0001F4B0 Moliyaviy yordam": {
        "title": "Moliyaviy yordam",
        "prompt": (
            "\U0001F4B0 Moliyaviy yordam bo'limi\n\n"
            "Moliyaviy yordam bo'yicha so'rovingizni batafsil yozing.\n\n"
            "Quyidagilarni kiriting:\n"
            "1. Yordam nima uchun kerakligi\n"
            "2. Oilaviy yoki ijtimoiy holat haqida qisqa ma'lumot\n"
            "3. Bog'lanish uchun telefon raqam\n\n"
            "Eslatma: murojaat mas'ullar tomonidan ko'rib chiqiladi."
        ),
        "success": (
            "Moliyaviy yordam bo'yicha so'rovingiz qabul qilindi. \U00002705\n\n"
            "Ma'lumotingiz mas'ullarga yuborildi. "
            "Holatingiz ko'rib chiqiladi."
        ),
    },
    "\U0001F4BC Ish o'rinlari": {
        "title": "Ish o'rinlari",
        "prompt": (
            "\U0001F4BC Ish o'rinlari bo'limi\n\n"
            "Ish bo'yicha murojaatingizni yozing.\n\n"
            "Quyidagilarni kiriting:\n"
            "1. Qaysi ish yoki yo'nalish qiziqtiradi\n"
            "2. Yoshingiz va tajribangiz\n"
            "3. Telefon raqamingiz\n\n"
            "Ma'lumotingizni hozir yuboring \U0001F447"
        ),
        "success": (
            "Ish o'rinlari bo'yicha so'rovingiz qabul qilindi. \U00002705\n\n"
            "Ma'lumotingiz mas'ullarga yuborildi. "
            "Mos imkoniyatlar bo'yicha ko'rib chiqiladi."
        ),
    },
    YOUTH_OPPORTUNITIES_SECTION_KEY: {
        "title": "Yoshlar imkoniyatlari",
        "prompt": (
            "\U0001F31F Yoshlar imkoniyatlari bo'limi\n\n"
            "Qaysi imkoniyat haqida ma'lumot kerakligini yozing.\n\n"
            "Masalan:\n"
            "1. Grant yoki tanlovlar\n"
            "2. Kurslar va o'quv markazlari\n"
            "3. Loyihalar, tadbirlar yoki volontyorlik\n\n"
            "Savolingizni hozir yuboring \U0001F447"
        ),
        "success": (
            "Yoshlar imkoniyatlari bo'yicha so'rovingiz qabul qilindi. \U00002705\n\n"
            "Savolingiz mas'ullarga yuborildi. "
            "Kerakli ma'lumotlar ko'rib chiqiladi."
        ),
    },
}

DEFAULT_BOT_COMMANDS = [
    BotCommand(command="start", description="Botni ishga tushirish"),
    BotCommand(command="help", description="Yordam va komandalar"),
    BotCommand(command="menu", description="Asosiy menyuni ochish"),
    BotCommand(command="region", description="Hududni qayta tanlash"),
    BotCommand(command="cancel", description="Joriy amalni bekor qilish"),
]

ADMIN_BOT_COMMAND = BotCommand(command="admin", description="Admin panelni ochish")
DEFAULT_SCRIPT = "latin"
SUPPORTED_SCRIPTS = {DEFAULT_SCRIPT, "cyrillic"}
CHANGE_REGION_BUTTON_TEXT = "\U0001F4CD Hududni o'zgartirish"
BACK_TO_MENU_BUTTON_TEXT = "\U00002B05 Asosiy menyu"
PHONE_SHARE_BUTTON_TEXT = "\U0001F4F1 Telefon raqamni yuborish"
JOIN_CHANNEL_BUTTON_TEXT = "\U0001F4E2 Kanalga qo'shilish"
CHECK_SUBSCRIPTION_BUTTON_TEXT = "\U00002705 Tekshirish"
ADMIN_REPLY_BUTTON_TEXT = "\U00002709 Javob berish"
ADMIN_OPEN_ATTACHMENT_BUTTON_TEXT = "\U0001F517 Qo'shimcha faylni ochish"
GLOBAL_STATS_BUTTON_TEXT = "\U0001F4CA Statistika bo'limi"
GLOBAL_STATS_RED_BUTTON_TEXT = "\U0001F534 Qizil hududlar"
GLOBAL_STATS_YELLOW_BUTTON_TEXT = "\U0001F7E1 Sariq hududlar"
GLOBAL_STATS_GREEN_BUTTON_TEXT = "\U0001F7E2 Yashil hududlar"
UPLOAD_ADDITIONAL_INFO_BUTTON_TEXT = "\U0001F4CE Ha, yuklayman"
SKIP_ADDITIONAL_INFO_BUTTON_TEXT = "\U000023ED O'tkazib yuborish"
HELP_SCRIPT_BUTTON_TEXTS = {
    "latin": "\U0001F524 Lotincha",
    "cyrillic": "\U0001F520 Kirillcha",
}
UI_SCREEN_KEY = "ui_screen"
UI_SCREEN_MAIN_MENU = "main_menu"
UI_SCREEN_REGION_SELECTION = "region_selection"
UI_SCREEN_SUBSCRIPTION_PROMPT = "subscription_prompt"
UI_SCREEN_STAFF_HOME = "staff_home"
UI_SCREEN_GLOBAL_STATS = "global_stats"
UI_SCREEN_ATTACHMENT_CHOICE = "attachment_choice"
UI_SCREEN_ATTACHMENT_UPLOAD = "attachment_upload"
UI_SCREEN_ADMIN_REPLY_WRITING = "admin_reply_writing"
UI_SCREEN_YOUTH_OPPORTUNITIES = "youth_opportunities"
UI_SCREEN_YOUTH_OPPORTUNITY_INFO = "youth_opportunity_info"
PROTECTED_TEXT_RE = re.compile(r"https?://\S+|/[A-Za-z0-9_]+|@[A-Za-z0-9_]+")
APOSTROPHE_TRANSLATION = str.maketrans(
    {
        "\u02bb": "'",
        "\u02bc": "'",
        "\u2018": "'",
        "\u2019": "'",
        "`": "'",
    }
)
MULTI_CHAR_CYRILLIC_MAP = (
    ("O'", "\u040e"),
    ("o'", "\u045e"),
    ("G'", "\u0492"),
    ("g'", "\u0493"),
    ("Sh", "\u0428"),
    ("sh", "\u0448"),
    ("CH", "\u0427"),
    ("Ch", "\u0427"),
    ("ch", "\u0447"),
    ("YA", "\u042f"),
    ("Ya", "\u042f"),
    ("ya", "\u044f"),
    ("YO", "\u0401"),
    ("Yo", "\u0401"),
    ("yo", "\u0451"),
    ("YU", "\u042e"),
    ("Yu", "\u042e"),
    ("yu", "\u044e"),
    ("YE", "\u0415"),
    ("Ye", "\u0415"),
    ("ye", "\u0435"),
)
SINGLE_CHAR_CYRILLIC_MAP = {
    "A": "\u0410",
    "a": "\u0430",
    "B": "\u0411",
    "b": "\u0431",
    "D": "\u0414",
    "d": "\u0434",
    "E": "\u0415",
    "e": "\u0435",
    "F": "\u0424",
    "f": "\u0444",
    "G": "\u0413",
    "g": "\u0433",
    "H": "\u04b2",
    "h": "\u04b3",
    "I": "\u0418",
    "i": "\u0438",
    "J": "\u0416",
    "j": "\u0436",
    "K": "\u041a",
    "k": "\u043a",
    "L": "\u041b",
    "l": "\u043b",
    "M": "\u041c",
    "m": "\u043c",
    "N": "\u041d",
    "n": "\u043d",
    "O": "\u041e",
    "o": "\u043e",
    "P": "\u041f",
    "p": "\u043f",
    "Q": "\u049a",
    "q": "\u049b",
    "R": "\u0420",
    "r": "\u0440",
    "S": "\u0421",
    "s": "\u0441",
    "T": "\u0422",
    "t": "\u0442",
    "U": "\u0423",
    "u": "\u0443",
    "V": "\u0412",
    "v": "\u0432",
    "X": "\u0425",
    "x": "\u0445",
    "Y": "\u0419",
    "y": "\u0439",
    "Z": "\u0417",
    "z": "\u0437",
    "C": "\u0421",
    "c": "\u0441",
    "'": "\u044a",
}


def extract_message_user_id(message: Message | None, fallback_user_id: int | None = None) -> int | None:
    if fallback_user_id is not None:
        return fallback_user_id

    if message and message.from_user and not message.from_user.is_bot:
        return message.from_user.id

    return None


def transliterate_segment_to_cyrillic(text: str) -> str:
    normalized = text.translate(APOSTROPHE_TRANSLATION)
    result: list[str] = []
    index = 0

    while index < len(normalized):
        for latin_text, cyrillic_text in MULTI_CHAR_CYRILLIC_MAP:
            if normalized.startswith(latin_text, index):
                result.append(cyrillic_text)
                index += len(latin_text)
                break
        else:
            char = normalized[index]
            result.append(SINGLE_CHAR_CYRILLIC_MAP.get(char, char))
            index += 1

    return "".join(result)


@lru_cache(maxsize=4096)
def apply_script_to_text(text: str, script: str) -> str:
    if script != "cyrillic" or not text:
        return text

    result: list[str] = []
    last_index = 0

    for match in PROTECTED_TEXT_RE.finditer(text):
        result.append(transliterate_segment_to_cyrillic(text[last_index:match.start()]))
        result.append(match.group(0))
        last_index = match.end()

    result.append(transliterate_segment_to_cyrillic(text[last_index:]))
    return "".join(result)


def build_localized_bot_commands(script: str, is_admin: bool = False) -> list[BotCommand]:
    commands = [
        BotCommand(
            command=command.command,
            description=apply_script_to_text(command.description, script),
        )
        for command in DEFAULT_BOT_COMMANDS
    ]

    if is_admin:
        commands.append(
            BotCommand(
                command=ADMIN_BOT_COMMAND.command,
                description=apply_script_to_text(ADMIN_BOT_COMMAND.description, script),
            )
        )

    return commands


def is_global_admin(user_id: int | None) -> bool:
    return ADMIN_USER_ID is not None and user_id == ADMIN_USER_ID


def is_region_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in REGION_ADMIN_USER_IDS


def is_primary_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in PRIMARY_ADMIN_USER_IDS


def is_staff_user(user_id: int | None) -> bool:
    return is_primary_admin(user_id)


def resolve_staff_user_id_for_region(region: str | None) -> int | None:
    canonical_region = resolve_region_name(region)
    if canonical_region:
        return REGION_LEADER_IDS.get(canonical_region)

    return None


def get_staff_regions(user_id: int | None) -> list[str]:
    if user_id is None:
        return []

    return [region for region in QAMASHI_HUDUDLARI if resolve_staff_user_id_for_region(region) == user_id]


def get_admin_scope_regions(user_id: int | None) -> list[str] | None:
    if not is_primary_admin(user_id):
        return []

    if is_global_admin(user_id):
        return None

    return get_staff_regions(user_id)


def resolve_request_staff_user_id(request_data: dict[str, object] | None) -> int | None:
    if not request_data:
        return None

    stored_staff_user_id = request_data.get("staff_user_id")
    if isinstance(stored_staff_user_id, int):
        return stored_staff_user_id

    if isinstance(stored_staff_user_id, str) and stored_staff_user_id.isdigit():
        return int(stored_staff_user_id)

    return resolve_staff_user_id_for_region(str(request_data.get("region", "")))


def can_staff_reply_to_request(user_id: int | None, request_data: dict[str, object] | None) -> bool:
    return is_region_admin(user_id) and resolve_request_staff_user_id(request_data) == user_id


def can_staff_view_request(user_id: int | None, request_data: dict[str, object] | None) -> bool:
    if is_global_admin(user_id):
        return True
    return can_staff_reply_to_request(user_id, request_data)


def get_staff_primary_region(user_id: int | None) -> str | None:
    managed_regions = get_staff_regions(user_id)
    return managed_regions[0] if managed_regions else None


def format_staff_contact_line(contact_value: str | None) -> str:
    value = str(contact_value or "").strip()
    return f"\n\U0001F517 Bog'lanish: {value}" if value else ""


def build_staff_home_text(user_id: int) -> str:
    if is_global_admin(user_id):
        admin_name = ADMIN_NAME or "Glavniy admin"
        region = get_staff_primary_region(user_id)
        region_text = (
            f"\n\n\U0001F31F Siz {region} Yoshlar yetakchisi sifatida ham biriktirilgansiz."
            if region
            else ""
        )
        region_contact = format_staff_contact_line(REGION_LEADER_URLS.get(region)) if region else ""
        return (
            "Assalomu Alaykum! \U0001F44B\n\n"
            f"\U0001F31F {admin_name}\n\n"
            "\U0001F451 Siz glavniy kuzatuvchisiz.\n"
            "\U0001F4E1 Siz barcha hududlardan kelgan murojaatlarni ko'rib turasiz.\n"
            "\U0001F4CA MFYlar holatini ko'rish uchun Statistika bo'limi tugmasidan foydalaning.\n"
            "\U0001F4CB Umumiy panel va oxirgi murojaatlar uchun /admin buyrug'idan foydalaning.\n"
            "\U0001F512 Bu akkaunt faqat kuzatish va umumiy nazorat uchun ishlaydi."
            f"{region_text}"
            f"{format_staff_contact_line(ADMIN_URL)}"
            f"{region_contact}"
        )

    if is_region_admin(user_id):
        region = get_staff_primary_region(user_id) or "Noma'lum hudud"
        return (
            "Assalomu alaykum! \U0001F44B\n\n"
            f"\U0001F31F Siz {region} Yoshlar yetakchisisiz.\n\n"
            f"\U0001F4CD Sizga faqat {region} hududidan kelgan murojaatlar yuboriladi.\n"
            "\U0001F4AC Javobni ham faqat siz qaytarasiz.\n"
            "Bu akkauntdan murojaat yuborish o'chirilgan.\n"
            "Sizga kelgan murojaatlarni ko'rib, kerak bo'lsa javob berishingiz mumkin.\n"
            "Statistika uchun /admin buyrug'idan foydalaning.\n"
            "Murojaat raqami bo'yicha javob yozish uchun /raqam ko'rinishidan foydalanishingiz mumkin.\n"
            "Masalan: /12"
            f"{format_staff_contact_line(REGION_LEADER_URLS.get(region))}"
        )

    return "Siz uchun staff sahifasi topilmadi."


def build_admin_reply_help_text() -> str:
    return (
        "Javob berish uchun xabarga reply qiling yoki /murojaat_raqami yuboring.\n"
        "Masalan: /12"
    )


def classify_region_signal(unanswered_count: int) -> tuple[str, str]:
    if unanswered_count >= 5:
        return "\U0001F534", "Qizil"
    if unanswered_count >= 2:
        return "\U0001F7E1", "Sariq"
    return "\U0001F7E2", "Yashil"


def format_region_status_entry(region: str, unanswered_count: int) -> str:
    signal_icon, _ = classify_region_signal(unanswered_count)
    return f"{signal_icon} {region}\n   Javobsiz murojaat: {unanswered_count} ta"


def group_regions_by_signal(region_counts: dict[str, int]) -> dict[str, list[tuple[str, int]]]:
    grouped_regions: dict[str, list[tuple[str, int]]] = {
        "Qizil": [],
        "Sariq": [],
        "Yashil": [],
    }

    for region in QAMASHI_HUDUDLARI:
        unanswered_count = int(region_counts.get(region, 0))
        _, signal_name = classify_region_signal(unanswered_count)
        grouped_regions[signal_name].append((region, unanswered_count))

    for regions in grouped_regions.values():
        regions.sort(key=lambda item: (-item[1], item[0]))

    return grouped_regions


def build_global_stats_text(region_counts: dict[str, int]) -> str:
    grouped_regions = group_regions_by_signal(region_counts)

    lines = [
        "\U0001F4CA Statistika bo'limi",
        "",
        "Quyidagi holatlardan birini tanlang \U0001F447",
        "",
        f"\U0001F534 Qizil hududlar: {len(grouped_regions['Qizil'])} ta",
        "5 ta va undan ko'p javobsiz murojaat",
        "",
        f"\U0001F7E1 Sariq hududlar: {len(grouped_regions['Sariq'])} ta",
        "2 tadan 4 tagacha javobsiz murojaat",
        "",
        f"\U0001F7E2 Yashil hududlar: {len(grouped_regions['Yashil'])} ta",
        "0 yoki 1 ta javobsiz murojaat",
    ]

    return "\n".join(lines).strip()


def resolve_global_stats_signal(button_text: str | None) -> str | None:
    button_map = {
        GLOBAL_STATS_RED_BUTTON_TEXT: "Qizil",
        GLOBAL_STATS_YELLOW_BUTTON_TEXT: "Sariq",
        GLOBAL_STATS_GREEN_BUTTON_TEXT: "Yashil",
    }
    for button_label, signal_name in button_map.items():
        if matches_localized_text(button_text, button_label):
            return signal_name
    return None


def build_global_stats_group_text(signal_name: str, region_counts: dict[str, int]) -> str:
    grouped_regions = group_regions_by_signal(region_counts)
    signal_icon = {
        "Qizil": "\U0001F534",
        "Sariq": "\U0001F7E1",
        "Yashil": "\U0001F7E2",
    }.get(signal_name, "\U0001F4CA")
    regions = grouped_regions.get(signal_name, [])

    lines = [
        f"{signal_icon} {signal_name} hududlar holati",
        "",
    ]

    if signal_name == "Qizil":
        lines.append("5 ta va undan ko'p javobsiz murojaat bor hududlar:")
    elif signal_name == "Sariq":
        lines.append("2 tadan 4 tagacha javobsiz murojaat bor hududlar:")
    else:
        lines.append("0 yoki 1 ta javobsiz murojaat bor hududlar:")

    lines.append("")

    if not regions:
        lines.append("Hozircha bu toifada hudud yo'q.")
        return "\n".join(lines)

    for region, unanswered_count in regions:
        lines.append(format_region_status_entry(region, unanswered_count))
        lines.append("")

    return "\n".join(lines).strip()


def parse_request_shortcut(message_text: str) -> int | None:
    value = (message_text or "").strip()

    if not value or " " in value or not value.startswith("/"):
        return None

    command = value.split("@", maxsplit=1)[0][1:]

    if not command.isdigit():
        return None

    return int(command)


def parse_request_id_from_admin_text(message_text: str | None) -> int | None:
    match = re.search(r"#(\d+)", str(message_text or ""))
    return int(match.group(1)) if match else None


def build_request_selection_text(request_data: dict[str, object]) -> str:
    preview = str(request_data.get("message", "")).replace("\n", " ")
    if len(preview) > 100:
        preview = preview[:100] + "..."

    return (
        f"#{request_data['id']} murojaat tanlandi. \U0001F4DD\n\n"
        f"\U0001F4C2 Bo'lim: {request_data['section']}\n"
        f"\U0001F464 Foydalanuvchi: {request_data['full_name']}\n"
        f"\U0001F4F1 Telefon: {request_data['phone']}\n"
        f"\U0001F4CD MFY: {request_data['region']}\n"
        f"\U00002753 Qisqa mazmun: {preview}\n\n"
        "\U0000270D Endi foydalanuvchiga yuboriladigan javob matnini yozing."
    )


def supports_optional_attachment(section_key: str) -> bool:
    return section_key in OPTIONAL_ATTACHMENT_SECTION_KEYS


def get_attachment_type_label(attachment_type: str | None) -> str:
    labels = {
        "photo": "\U0001F5BC Foto",
        "video": "\U0001F3A5 Video",
        "document": "\U0001F4C4 Hujjat",
    }
    return labels.get(str(attachment_type or "").strip().lower(), "\U0001F4CE Fayl")


def build_optional_attachment_choice_text(section_key: str) -> str:
    section = SECTION_CONFIG.get(section_key, SECTION_CONFIG["\U0001F4E9 Murojaat yuborish"])
    return (
        f"\u2728 {section['title']} bo'limi\n\n"
        "\U0001F4CE Qo'shimcha ma'lumot yuklaysizmi?\n\n"
        "Agar xohlasangiz, foto, video yoki hujjat yuborishingiz mumkin.\n"
        "Bu mas'ullarga vaziyatni tezroq tushunishga yordam beradi.\n\n"
        "Quyidagilardan birini tanlang \U0001F447"
    )


def build_attachment_upload_text(section_key: str) -> str:
    section = SECTION_CONFIG.get(section_key, SECTION_CONFIG["\U0001F4E9 Murojaat yuborish"])
    return (
        f"\U0001F4CE {section['title']} uchun qo'shimcha ma'lumot yuborish\n\n"
        "Bitta foto, video yoki hujjat yuboring.\n"
        "Agar fikringiz o'zgarsa, pastdagi `O'tkazib yuborish` tugmasini bosing. \U000023ED"
    )


def build_attachment_saved_notice(attachment_type: str | None) -> str:
    return (
        f"\U00002705 Qo'shimcha ma'lumot saqlandi: {get_attachment_type_label(attachment_type)}\n\n"
        "Endi asosiy matnni yozing \U0000270D"
    )


def build_youth_opportunities_menu_text() -> str:
    return (
        "\U0001F31F Yoshlar imkoniyatlari\n\n"
        "Quyidagi yo'nalishlardan birini tanlang:\n"
        "\U0001F4DA Ta'lim\n"
        "\U0001F4BC Ish\n"
        "\U0001F680 Startap\n"
        "\U0001F30D Tanlovlar\n"
        "\U0001F9E0 Rivojlanish\n\n"
        "Kerakli tugmani bosing \U0001F447"
    )


def build_message_with_attachment_note(message_text: str, attachment_type: str | None) -> str:
    cleaned_text = message_text.strip()
    if not attachment_type:
        return cleaned_text
    return f"{cleaned_text}\n\n[\U0001F4CE Qo'shimcha ma'lumot biriktirilgan: {get_attachment_type_label(attachment_type)}]"


def build_attachment_admin_note(attachment_type: str | None, attachment_caption: str | None = None) -> str:
    if not attachment_type:
        return ""

    note = f"\n\n\U0001F517 {get_attachment_type_label(attachment_type)} pastdagi tugma orqali ochiladi."
    if attachment_caption:
        note += f"\n\U0001F4DD Qo'shimcha izoh: {attachment_caption.strip()}"
    return note


def build_subscription_prompt_text() -> str:
    return (
        "Botdan foydalanishni davom ettirish uchun avval rasmiy kanalga qo'shiling. \U0001F4E2\n\n"
        "Kanalga a'zo bo'lgach, `Tekshirish` tugmasini bosing."
    )


def resolve_script_choice(message_text: str | None) -> str | None:
    for script, button_text in HELP_SCRIPT_BUTTON_TEXTS.items():
        if matches_localized_text(message_text, button_text):
            return script

    return None


async def start_admin_reply_by_request_id(
    message: Message,
    state: FSMContext,
    request_id: int,
    user_id: int | None = None,
) -> bool:
    resolved_user_id = extract_message_user_id(message, user_id)
    request_data = get_request_by_id(request_id)

    if not request_data:
        await message.answer(localize_text_for_user(resolved_user_id, f"#{request_id} murojaat topilmadi."))
        return False

    if not can_staff_reply_to_request(resolved_user_id, request_data):
        await message.answer(localize_text_for_user(resolved_user_id, "Bu murojaat sizga biriktirilmagan."))
        return False

    await state.set_state(AdminReplyState.waiting_for_reply)
    await state.update_data(
        reply_request_id=request_id,
        **{UI_SCREEN_KEY: UI_SCREEN_ADMIN_REPLY_WRITING},
    )
    await message.answer(
        localize_text_for_user(resolved_user_id, build_request_selection_text(request_data)),
        reply_markup=ReplyKeyboardRemove(),
    )
    return True


async def send_admin_reply_to_user(
    bot: Bot,
    request_data: dict[str, object],
    message_text: str,
    answered_by: int | None = None,
) -> str:
    user_id = int(request_data["user_id"])
    user_text = (
        f"\U0001F4E8 Murojaatingiz bo'yicha javob #{request_data['id']}\n\n"
        f"\U0001F4C2 Bo'lim: {request_data['section']}\n\n"
        f"\U0001F4AC Javob:\n{message_text}"
    )

    await bot.send_message(chat_id=user_id, text=localize_text_for_user(user_id, user_text))
    mark_request_answered(int(request_data["id"]), answered_by)
    return "Javob foydalanuvchiga yuborildi. \U00002705"


async def send_request_attachment_on_demand(
    bot: Bot,
    chat_id: int,
    request_data: dict[str, object],
    attachment_type: str | None,
    attachment_file_id: str | None,
) -> None:
    if not attachment_type or not attachment_file_id:
        await bot.send_message(
            chat_id=chat_id,
            text=localize_text_for_user(chat_id, "Bu murojaatga qo'shimcha fayl biriktirilmagan."),
        )
        return

    attachment_type = attachment_type.strip().lower()
    request_id = int(request_data["id"])
    caption = localize_text_for_user(
        chat_id,
        (
            f"\U0001F4CE Qo'shimcha fayl #{request_id}\n"
            f"\U0001F4C2 Bo'lim: {request_data['section']}\n"
            f"\U0001F4CD MFY: {request_data['region']}\n"
            f"{get_attachment_type_label(attachment_type)}"
        ),
    )

    if attachment_type == "photo":
        downloaded_file = await bot.download(attachment_file_id)
        if downloaded_file is None:
            await bot.send_message(chat_id=chat_id, text=localize_text_for_user(chat_id, "Rasmni ochib bo'lmadi."))
            return
        file_bytes = downloaded_file.read()
        if not file_bytes:
            await bot.send_message(chat_id=chat_id, text=localize_text_for_user(chat_id, "Rasmni ochib bo'lmadi."))
            return
        await bot.send_document(
            chat_id=chat_id,
            document=BufferedInputFile(file_bytes, filename=f"murojaat_{request_id}.jpg"),
            caption=caption,
        )
        return

    if attachment_type == "video":
        await bot.send_video(
            chat_id=chat_id,
            video=attachment_file_id,
            caption=caption,
        )
        return

    if attachment_type == "document":
        await bot.send_document(
            chat_id=chat_id,
            document=attachment_file_id,
            caption=caption,
        )
        return

    await bot.send_message(
        chat_id=chat_id,
        text=localize_text_for_user(chat_id, "Qo'shimcha fayl turini ochib bo'lmadi."),
    )


async def notify_staff_about_request(
    bot: Bot,
    request_id: int,
    admin_text: str,
    staff_user_id: int | None,
    attachment_type: str | None = None,
    attachment_file_id: str | None = None,
) -> None:
    has_attachment = bool(attachment_type and attachment_file_id)
    if staff_user_id:
        try:
            sent_message = await bot.send_message(
                chat_id=staff_user_id,
                text=localize_text_for_user(staff_user_id, admin_text),
                reply_markup=build_admin_request_keyboard(
                    request_id,
                    get_user_script(staff_user_id),
                    allow_reply=True,
                    has_attachment=has_attachment,
                ),
            )
            set_request_admin_message_id(request_id, sent_message.message_id)
        except Exception as error:
            logging.warning(
                "Mas'ul yetakchiga murojaatni yuborib bo'lmadi. request_id=%s staff_user_id=%s error=%s",
                request_id,
                staff_user_id,
                error,
            )
    else:
        logging.warning("Murojaat uchun mas'ul yetakchi topilmadi. request_id=%s", request_id)

    if ADMIN_USER_ID and ADMIN_USER_ID != staff_user_id:
        try:
            reply_markup = (
                build_admin_request_keyboard(
                    request_id,
                    get_user_script(ADMIN_USER_ID),
                    allow_reply=False,
                    has_attachment=True,
                )
                if has_attachment
                else None
            )
            await bot.send_message(
                chat_id=ADMIN_USER_ID,
                text=localize_text_for_user(ADMIN_USER_ID, admin_text),
                reply_markup=reply_markup,
            )
        except Exception as error:
            logging.warning("Glavniy boshliqqa murojaatni yuborib bo'lmadi. request_id=%s error=%s", request_id, error)


async def sync_ui_after_script_change(
    message: Message,
    state: FSMContext,
    user_id: int,
) -> None:
    current_state = await state.get_state()
    data = await state.get_data()
    ui_screen = data.get(UI_SCREEN_KEY)

    if is_staff_user(user_id):
        if current_state == AdminReplyState.waiting_for_reply.state:
            await message.answer(
                localize_text_for_user(
                    user_id,
                    "Javob yozish rejimi faol.\n\nFoydalanuvchiga yuboriladigan javob matnini yozing.",
                ),
                reply_markup=ReplyKeyboardRemove(),
            )
            return

        if ui_screen == UI_SCREEN_GLOBAL_STATS and is_global_admin(user_id):
            signal_name = data.get("global_stats_signal")
            if signal_name:
                await show_global_stats_group(message, state, str(signal_name), user_id=user_id)
            else:
                await show_global_stats_dashboard(message, state, user_id=user_id)
            return

        await show_staff_home(message, state, user_id=user_id)
        return

    if current_state == RegisterState.waiting_for_full_name.state:
        await ask_full_name(message, state, user_id=user_id)
        return

    if current_state == RegisterState.waiting_for_phone.state:
        await ask_phone(message, state, user_id=user_id)
        return

    if current_state == AppealState.waiting_for_attachment_choice.state:
        active_section = data.get("active_section", "\U0001F4E9 Murojaat yuborish")
        await show_optional_attachment_choice(message, state, active_section, user_id=user_id)
        return

    if current_state == AppealState.waiting_for_attachment.state:
        active_section = data.get("active_section", "\U0001F4E9 Murojaat yuborish")
        await show_attachment_upload_prompt(message, state, active_section, user_id=user_id)
        return

    if current_state == AppealState.waiting_for_section_text.state:
        active_section = data.get("active_section", "\U0001F4E9 Murojaat yuborish")
        await show_section_text_prompt(message, state, active_section, user_id=user_id)
        return

    pending_section = data.get("pending_section")
    pending_youth_opportunity_key = data.get("pending_youth_opportunity_key")
    if ui_screen == UI_SCREEN_SUBSCRIPTION_PROMPT or pending_section:
        await send_subscription_prompt(
            message,
            state,
            pending_section,
            user_id=user_id,
            pending_youth_opportunity_key=(
                str(pending_youth_opportunity_key)
                if pending_youth_opportunity_key in YOUTH_OPPORTUNITY_CONFIG
                else None
            ),
        )
        return

    user = get_user(user_id)
    if not is_user_registered(user):
        await ask_full_name(message, state, user_id=user_id)
        return

    if ui_screen == UI_SCREEN_REGION_SELECTION or not user.get("region"):
        await ask_region(message, state, user_id=user_id)
        return

    if get_cached_subscription_status(user_id) is False:
        await send_subscription_prompt(message, state, user_id=user_id)
        return

    if ui_screen == UI_SCREEN_YOUTH_OPPORTUNITIES:
        await show_youth_opportunities_menu(message, state, user_id=user_id)
        return

    youth_opportunity_key = data.get("youth_opportunity_key")
    if ui_screen == UI_SCREEN_YOUTH_OPPORTUNITY_INFO and youth_opportunity_key in YOUTH_OPPORTUNITY_CONFIG:
        await show_youth_opportunity_info(message, state, str(youth_opportunity_key), user_id=user_id)
        return

    await show_main_menu(
        message,
        state,
        user_id=user_id,
        intro_text="Kerakli bo'limni tanlang \U0001F447",
    )


def get_mongo_collections():
    if (
        users_collection is None
        or requests_collection is None
        or counters_collection is None
        or meta_collection is None
    ):
        raise RuntimeError("MongoDB ulanishi hali ishga tushirilmagan")

    return users_collection, requests_collection, counters_collection, meta_collection


@contextmanager
def get_sqlite_connection():
    conn = sqlite3.connect(
        LEGACY_SQLITE_PATH,
        timeout=SQLITE_CONNECT_TIMEOUT_SECONDS,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_sqlite_db() -> None:
    with get_sqlite_connection() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT,
                phone TEXT,
                region TEXT,
                username TEXT,
                script TEXT DEFAULT 'latin',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                section TEXT NOT NULL,
                region TEXT NOT NULL,
                staff_user_id INTEGER,
                full_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                username TEXT NOT NULL,
                message TEXT NOT NULL,
                attachment_type TEXT,
                attachment_file_id TEXT,
                answered_by INTEGER,
                answered_at TEXT,
                admin_message_id INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_requests_user_id ON requests(user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_requests_staff_user_id ON requests(staff_user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_requests_admin_message_id ON requests(admin_message_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_requests_created_at ON requests(created_at DESC)"
        )

        user_columns = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        if "script" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN script TEXT DEFAULT 'latin'")

        request_columns = {row["name"] for row in conn.execute("PRAGMA table_info(requests)").fetchall()}
        if "staff_user_id" not in request_columns:
            conn.execute("ALTER TABLE requests ADD COLUMN staff_user_id INTEGER")
        if "attachment_type" not in request_columns:
            conn.execute("ALTER TABLE requests ADD COLUMN attachment_type TEXT")
        if "attachment_file_id" not in request_columns:
            conn.execute("ALTER TABLE requests ADD COLUMN attachment_file_id TEXT")
        if "answered_by" not in request_columns:
            conn.execute("ALTER TABLE requests ADD COLUMN answered_by INTEGER")
        if "answered_at" not in request_columns:
            conn.execute("ALTER TABLE requests ADD COLUMN answered_at TEXT")
        if "admin_message_id" not in request_columns:
            conn.execute("ALTER TABLE requests ADD COLUMN admin_message_id INTEGER")


def sync_request_counter() -> None:
    _, requests, counters, _ = get_mongo_collections()
    latest_request = requests.find_one({}, {"_id": 0, "id": 1}, sort=[("id", DESCENDING)])
    max_request_id = int(latest_request["id"]) if latest_request and latest_request.get("id") else 0
    counter = counters.find_one({"_id": "requests"}, {"_id": 0, "seq": 1})

    if not counter:
        counters.insert_one({"_id": "requests", "seq": max_request_id})
        return

    current_seq = int(counter.get("seq", 0))
    if current_seq < max_request_id:
        counters.update_one(
            {"_id": "requests"},
            {"$set": {"seq": max_request_id}},
        )


def migrate_sqlite_to_mongo_if_needed() -> None:
    users, requests, counters, meta = get_mongo_collections()

    if not os.path.exists(LEGACY_SQLITE_PATH):
        return

    if meta.find_one({"_id": "sqlite_migration"}):
        return

    with get_sqlite_connection() as conn:
        table_names = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }

        if "users" not in table_names and "requests" not in table_names:
            return

        user_rows: list[dict[str, object]] = []
        if "users" in table_names:
            user_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()
            }
            user_select_columns = [
                "user_id",
                "full_name",
                "phone",
                "region",
                "username",
                "created_at",
                "updated_at",
            ]
            if "script" in user_columns:
                user_select_columns.append("script")
            user_rows = [
                dict(row)
                for row in conn.execute(
                    f"SELECT {', '.join(user_select_columns)} FROM users"
                ).fetchall()
            ]

            for row in user_rows:
                row.setdefault("script", DEFAULT_SCRIPT)

        request_rows: list[dict[str, object]] = []
        if "requests" in table_names:
            request_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(requests)").fetchall()
            }
            select_columns = [
                "id",
                "user_id",
                "section",
                "region",
                "staff_user_id",
                "full_name",
                "phone",
                "username",
                "message",
                "created_at",
            ]

            if "attachment_type" in request_columns:
                select_columns.insert(-1, "attachment_type")
            if "attachment_file_id" in request_columns:
                select_columns.insert(-1, "attachment_file_id")
            if "answered_by" in request_columns:
                select_columns.insert(-1, "answered_by")
            if "answered_at" in request_columns:
                select_columns.insert(-1, "answered_at")
            if "admin_message_id" in request_columns:
                select_columns.insert(-1, "admin_message_id")

            request_rows = [
                dict(row)
                for row in conn.execute(
                    f"SELECT {', '.join(select_columns)} FROM requests"
                ).fetchall()
            ]

            for row in request_rows:
                row.setdefault("staff_user_id", None)
                row.setdefault("attachment_type", None)
                row.setdefault("attachment_file_id", None)
                row.setdefault("answered_by", None)
                row.setdefault("answered_at", None)
                row.setdefault("admin_message_id", None)

    if user_rows:
        users.bulk_write(
            [
                UpdateOne(
                    {"user_id": int(row["user_id"])},
                    {"$setOnInsert": row},
                    upsert=True,
                )
                for row in user_rows
            ],
            ordered=False,
        )

    if request_rows:
        requests.bulk_write(
            [
                UpdateOne(
                    {"id": int(row["id"])},
                    {"$setOnInsert": row},
                    upsert=True,
                )
                for row in request_rows
            ],
            ordered=False,
        )

    max_request_id = max((int(row["id"]) for row in request_rows), default=0)
    counters.update_one(
        {"_id": "requests"},
        {"$set": {"seq": max_request_id}},
        upsert=True,
    )
    meta.update_one(
        {"_id": "sqlite_migration"},
        {
            "$set": {
                "source_path": LEGACY_SQLITE_PATH,
                "users_count": len(user_rows),
                "requests_count": len(request_rows),
                "completed_at": datetime.now().isoformat(timespec="seconds"),
            }
        },
        upsert=True,
    )
    logging.info(
        "SQLite ma'lumotlari MongoDB'ga ko'chirildi. users=%s requests=%s",
        len(user_rows),
        len(request_rows),
    )


def init_db() -> None:
    global mongo_client, mongo_db, users_collection, requests_collection, counters_collection, meta_collection, DB_BACKEND
    USER_CACHE.clear()
    SUBSCRIPTION_CACHE.clear()

    try:
        effective_mongo_uri = build_effective_mongo_uri(MONGODB_URI)
        mongo_client = MongoClient(effective_mongo_uri, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command("ping")
        mongo_db = mongo_client[MONGODB_DB_NAME]
        users_collection = mongo_db["users"]
        requests_collection = mongo_db["requests"]
        counters_collection = mongo_db["counters"]
        meta_collection = mongo_db["meta"]

        users_collection.create_index([("user_id", ASCENDING)], unique=True)
        requests_collection.create_index([("id", ASCENDING)], unique=True)
        requests_collection.create_index([("user_id", ASCENDING)])
        requests_collection.create_index([("staff_user_id", ASCENDING)])
        requests_collection.create_index([("admin_message_id", ASCENDING)], sparse=True)
        requests_collection.create_index([("created_at", DESCENDING)])

        migrate_sqlite_to_mongo_if_needed()
        sync_request_counter()
        DB_BACKEND = "mongo"
        logging.info("MongoDB ulandi va ishga tushdi.")
    except Exception as error:
        DB_BACKEND = "sqlite"
        if mongo_client is not None:
            mongo_client.close()
        mongo_client = None
        mongo_db = None
        users_collection = None
        requests_collection = None
        counters_collection = None
        meta_collection = None
        init_sqlite_db()
        error_text = str(error)
        if "Authentication failed" in error_text or "bad auth" in error_text:
            logging.warning(
                "MongoDB TLS ulanishi ishladi, lekin MONGODB_URI ichidagi login yoki parol noto'g'ri. SQLite fallback yoqildi: %s",
                error,
            )
        else:
            logging.warning("MongoDB ulanmaganligi sababli SQLite fallback yoqildi: %s", error)


def get_user(user_id: int) -> dict[str, object] | None:
    cached_user = USER_CACHE.get(user_id)
    if cached_user is MISSING_USER:
        return None
    if isinstance(cached_user, dict):
        return cached_user

    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            row = conn.execute(
                """
                SELECT user_id, full_name, phone, region, username, script, created_at, updated_at
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            user = dict(row) if row else None
            USER_CACHE[user_id] = user if user is not None else MISSING_USER
            return user

    users, _, _, _ = get_mongo_collections()
    user = users.find_one({"user_id": user_id}, {"_id": 0})
    USER_CACHE[user_id] = user if user is not None else MISSING_USER
    return user


def get_user_script(user_id: int | None) -> str:
    if not user_id:
        return DEFAULT_SCRIPT

    user = get_user(user_id) or {}
    script = user.get("script")
    return script if script in SUPPORTED_SCRIPTS else DEFAULT_SCRIPT


def is_user_registered(user: dict[str, object] | None) -> bool:
    return bool(user and user.get("full_name") and user.get("phone"))


def localize_text_for_user(user_id: int | None, text: str) -> str:
    return apply_script_to_text(text, get_user_script(user_id))


def localize_text_for_script(script: str, text: str) -> str:
    return apply_script_to_text(text, script)


def matches_localized_text(text: str | None, canonical_text: str) -> bool:
    if not text:
        return False

    return text in {
        canonical_text,
        apply_script_to_text(canonical_text, "cyrillic"),
    }


def resolve_section_key(message_text: str | None) -> str | None:
    if not message_text:
        return None

    for section_key in SECTION_CONFIG:
        if matches_localized_text(message_text, section_key):
            return section_key

    return None


def resolve_youth_opportunity_key(message_text: str | None) -> str | None:
    if not message_text:
        return None

    for opportunity_key in YOUTH_OPPORTUNITY_CONFIG:
        if matches_localized_text(message_text, opportunity_key):
            return opportunity_key

    return None


def set_user_script(user_id: int, script: str) -> None:
    if script not in SUPPORTED_SCRIPTS:
        return

    now = datetime.now().isoformat(timespec="seconds")
    existing_user = get_user(user_id) or {}
    updated_user = {
        "user_id": user_id,
        "full_name": existing_user.get("full_name", ""),
        "phone": existing_user.get("phone", ""),
        "region": existing_user.get("region", ""),
        "username": existing_user.get("username", ""),
        "script": script,
        "created_at": existing_user.get("created_at", now),
        "updated_at": now,
    }
    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            if existing_user:
                conn.execute(
                    """
                    UPDATE users
                    SET script = ?, updated_at = ?
                    WHERE user_id = ?
                    """,
                    (script, now, user_id),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO users (user_id, full_name, phone, region, username, script, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (user_id, "", "", "", "", script, now, now),
                )
        USER_CACHE[user_id] = updated_user
        return

    users, _, _, _ = get_mongo_collections()
    users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "script": script,
                "updated_at": now,
            },
            "$setOnInsert": {
                "user_id": user_id,
                "created_at": now,
            },
        },
        upsert=True,
    )
    USER_CACHE[user_id] = updated_user


def save_user_profile(user_id: int, full_name: str, phone: str, username: str | None) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    existing_user = get_user(user_id)
    script = existing_user.get("script", DEFAULT_SCRIPT) if existing_user else DEFAULT_SCRIPT
    created_at = existing_user.get("created_at", now) if existing_user else now
    region = existing_user.get("region", "") if existing_user else ""
    updated_user = {
        "user_id": user_id,
        "full_name": full_name,
        "phone": phone,
        "region": region,
        "username": username or "",
        "script": script,
        "created_at": created_at,
        "updated_at": now,
    }
    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO users (
                    user_id, full_name, phone, region, username, script, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, full_name, phone, region, username or "", script, created_at, now),
            )
        USER_CACHE[user_id] = updated_user
        return

    users, _, _, _ = get_mongo_collections()
    users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "full_name": full_name,
                "phone": phone,
                "username": username,
                "updated_at": now,
            },
            "$setOnInsert": {
                "user_id": user_id,
                "created_at": now,
                "script": DEFAULT_SCRIPT,
            },
        },
        upsert=True,
    )
    USER_CACHE[user_id] = updated_user


def update_user_region(user_id: int, region: str) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    existing_user = get_user(user_id) or {}
    updated_user = {
        "user_id": user_id,
        "full_name": existing_user.get("full_name", ""),
        "phone": existing_user.get("phone", ""),
        "region": region,
        "username": existing_user.get("username", ""),
        "script": existing_user.get("script", DEFAULT_SCRIPT),
        "created_at": existing_user.get("created_at", now),
        "updated_at": now,
    }
    if DB_BACKEND == "sqlite":
        if not existing_user:
            with get_sqlite_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO users (user_id, full_name, phone, region, username, script, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (user_id, "", "", region, "", DEFAULT_SCRIPT, now, now),
                )
            USER_CACHE[user_id] = updated_user
            return

        with get_sqlite_connection() as conn:
            conn.execute(
                """
                UPDATE users
                SET region = ?, updated_at = ?
                WHERE user_id = ?
                """,
                (region, now, user_id),
            )
        USER_CACHE[user_id] = updated_user
        return

    users, _, _, _ = get_mongo_collections()
    users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "region": region,
                "updated_at": now,
            },
            "$setOnInsert": {
                "user_id": user_id,
                "created_at": now,
                "script": existing_user.get("script", DEFAULT_SCRIPT),
            },
        },
        upsert=True,
    )
    USER_CACHE[user_id] = updated_user


def get_next_request_id() -> int:
    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            row = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM requests").fetchone()
            return int(row["next_id"])

    _, _, counters, _ = get_mongo_collections()
    counter = counters.find_one_and_update(
        {"_id": "requests"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return int(counter["seq"])


def save_request(
    user_id: int,
    section: str,
    region: str,
    staff_user_id: int | None,
    full_name: str,
    phone: str,
    username: str,
    message: str,
    attachment_type: str | None = None,
    attachment_file_id: str | None = None,
) -> int:
    now = datetime.now().isoformat(timespec="seconds")
    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO requests (
                    user_id, section, region, staff_user_id, full_name, phone, username, message,
                    attachment_type, attachment_file_id, answered_by, answered_at, admin_message_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
                """,
                (
                    user_id,
                    section,
                    region,
                    staff_user_id,
                    full_name,
                    phone,
                    username,
                    message,
                    attachment_type,
                    attachment_file_id,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    _, requests, _, _ = get_mongo_collections()
    request_id = get_next_request_id()
    requests.insert_one(
        {
            "id": request_id,
            "user_id": user_id,
            "section": section,
            "region": region,
            "staff_user_id": staff_user_id,
            "full_name": full_name,
            "phone": phone,
            "username": username,
            "message": message,
            "attachment_type": attachment_type,
            "attachment_file_id": attachment_file_id,
            "answered_by": None,
            "answered_at": None,
            "admin_message_id": None,
            "created_at": now,
        }
    )
    return request_id


def set_request_admin_message_id(request_id: int, admin_message_id: int) -> None:
    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            conn.execute(
                "UPDATE requests SET admin_message_id = ? WHERE id = ?",
                (admin_message_id, request_id),
            )
        return

    _, requests, _, _ = get_mongo_collections()
    requests.update_one(
        {"id": request_id},
        {"$set": {"admin_message_id": admin_message_id}},
    )


def get_request_by_admin_message(admin_message_id: int) -> dict[str, object] | None:
    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            row = conn.execute(
                """
                SELECT id, user_id, section, region, staff_user_id, full_name, phone, username, message,
                       attachment_type, attachment_file_id, answered_by, answered_at, admin_message_id, created_at
                FROM requests
                WHERE admin_message_id = ?
                """,
                (admin_message_id,),
            ).fetchone()
            return dict(row) if row else None

    _, requests, _, _ = get_mongo_collections()
    return requests.find_one({"admin_message_id": admin_message_id}, {"_id": 0})


def get_request_by_id(request_id: int) -> dict[str, object] | None:
    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            row = conn.execute(
                """
                SELECT id, user_id, section, region, staff_user_id, full_name, phone, username, message,
                       attachment_type, attachment_file_id, answered_by, answered_at, admin_message_id, created_at
                FROM requests
                WHERE id = ?
                """,
                (request_id,),
            ).fetchone()
            return dict(row) if row else None

    _, requests, _, _ = get_mongo_collections()
    return requests.find_one({"id": request_id}, {"_id": 0})


def mark_request_answered(request_id: int, answered_by: int | None) -> None:
    answered_at = datetime.now().isoformat(timespec="seconds")
    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            conn.execute(
                """
                UPDATE requests
                SET answered_by = ?, answered_at = ?
                WHERE id = ?
                """,
                (answered_by, answered_at, request_id),
            )
        return

    _, requests, _, _ = get_mongo_collections()
    requests.update_one(
        {"id": request_id},
        {"$set": {"answered_by": answered_by, "answered_at": answered_at}},
    )


def get_unanswered_request_counts_by_region() -> dict[str, int]:
    counts = {region: 0 for region in QAMASHI_HUDUDLARI}

    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            rows = conn.execute(
                """
                SELECT region, COUNT(*) AS unanswered_count
                FROM requests
                WHERE answered_at IS NULL OR answered_at = ''
                GROUP BY region
                """
            ).fetchall()
            for row in rows:
                region = resolve_region_name(row["region"]) or str(row["region"])
                if region in counts:
                    counts[region] = int(row["unanswered_count"])
        return counts

    _, requests, _, _ = get_mongo_collections()
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"answered_at": {"$exists": False}},
                    {"answered_at": None},
                    {"answered_at": ""},
                ]
            }
        },
        {
            "$group": {
                "_id": "$region",
                "unanswered_count": {"$sum": 1},
            }
        },
    ]
    for row in requests.aggregate(pipeline):
        region = resolve_region_name(str(row.get("_id", ""))) or str(row.get("_id", ""))
        if region in counts:
            counts[region] = int(row.get("unanswered_count", 0))
    return counts


def get_admin_stats(user_id: int | None = None) -> dict[str, int]:
    managed_regions = get_admin_scope_regions(user_id) if user_id and is_primary_admin(user_id) else None

    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            if managed_regions is None:
                users_count = int(conn.execute("SELECT COUNT(*) AS count FROM users").fetchone()["count"])
                requests_count = int(conn.execute("SELECT COUNT(*) AS count FROM requests").fetchone()["count"])
                return {"users_count": users_count, "requests_count": requests_count}

            if not managed_regions:
                return {"users_count": 0, "requests_count": 0}

            placeholders = ", ".join("?" for _ in managed_regions)
            users_count = int(
                conn.execute(
                    f"SELECT COUNT(*) AS count FROM users WHERE region IN ({placeholders})",
                    tuple(managed_regions),
                ).fetchone()["count"]
            )
            requests_count = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*) AS count
                    FROM requests
                    WHERE staff_user_id = ?
                    OR (staff_user_id IS NULL AND region IN ({placeholders}))
                    """,
                    (user_id, *managed_regions),
                ).fetchone()["count"]
            )
            return {"users_count": users_count, "requests_count": requests_count}

    users, requests, _, _ = get_mongo_collections()
    if managed_regions is None:
        users_count = users.count_documents({})
        requests_count = requests.count_documents({})
        return {"users_count": users_count, "requests_count": requests_count}

    if not managed_regions:
        return {"users_count": 0, "requests_count": 0}

    users_count = users.count_documents({"region": {"$in": managed_regions}})
    requests_count = requests.count_documents(
        {
            "$or": [
                {"staff_user_id": user_id},
                {"staff_user_id": {"$exists": False}, "region": {"$in": managed_regions}},
                {"staff_user_id": None, "region": {"$in": managed_regions}},
            ]
        }
    )
    return {"users_count": users_count, "requests_count": requests_count}


def get_latest_requests(limit: int = 5, user_id: int | None = None) -> list[dict[str, object]]:
    managed_regions = get_admin_scope_regions(user_id) if user_id and is_primary_admin(user_id) else None

    if DB_BACKEND == "sqlite":
        with get_sqlite_connection() as conn:
            if managed_regions is None:
                rows = conn.execute(
                    """
                    SELECT id, section, region, full_name, phone, message, created_at
                    FROM requests
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
                return [dict(row) for row in rows]

            if not managed_regions:
                return []

            placeholders = ", ".join("?" for _ in managed_regions)
            rows = conn.execute(
                f"""
                SELECT id, section, region, full_name, phone, message, created_at
                FROM requests
                WHERE staff_user_id = ?
                OR (staff_user_id IS NULL AND region IN ({placeholders}))
                ORDER BY id DESC
                LIMIT ?
                """,
                (user_id, *managed_regions, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    _, requests, _, _ = get_mongo_collections()
    query = {}
    if managed_regions is not None:
        if not managed_regions:
            return []

        query = {
            "$or": [
                {"staff_user_id": user_id},
                {"staff_user_id": {"$exists": False}, "region": {"$in": managed_regions}},
                {"staff_user_id": None, "region": {"$in": managed_regions}},
            ]
        }

    cursor = requests.find(
        query,
        {
            "_id": 0,
            "id": 1,
            "section": 1,
            "region": 1,
            "full_name": 1,
            "phone": 1,
            "message": 1,
            "created_at": 1,
        },
    ).sort("id", DESCENDING).limit(limit)
    return list(cursor)


def remember_subscription_status(user_id: int, is_subscribed: bool) -> None:
    ttl = SUBSCRIPTION_CACHE_TTL_TRUE_SECONDS if is_subscribed else SUBSCRIPTION_CACHE_TTL_FALSE_SECONDS
    SUBSCRIPTION_CACHE[user_id] = (is_subscribed, monotonic() + ttl)


def get_cached_subscription_status(user_id: int) -> bool | None:
    cached_status = SUBSCRIPTION_CACHE.get(user_id)
    if not cached_status:
        return None

    is_subscribed, expires_at = cached_status
    if expires_at <= monotonic():
        SUBSCRIPTION_CACHE.pop(user_id, None)
        return None

    return is_subscribed


@lru_cache(maxsize=2)
def build_region_keyboard(script: str = DEFAULT_SCRIPT) -> InlineKeyboardMarkup:
    buttons = []
    row = []

    for code, region in REGION_CODES.items():
        row.append(
            InlineKeyboardButton(
                text=localize_text_for_script(script, f"\U0001F3E1 {region}"),
                callback_data=f"region:{code}",
            )
        )
        if len(row) == 3:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    return InlineKeyboardMarkup(inline_keyboard=buttons)


@lru_cache(maxsize=2)
def build_subscription_keyboard(script: str = DEFAULT_SCRIPT) -> InlineKeyboardMarkup:
    channel_url = build_channel_url()
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=localize_text_for_script(script, JOIN_CHANNEL_BUTTON_TEXT),
                    url=channel_url,
                )
            ],
            [
                InlineKeyboardButton(
                    text=localize_text_for_script(script, CHECK_SUBSCRIPTION_BUTTON_TEXT),
                    callback_data="check_subscription",
                )
            ],
        ]
    )


@lru_cache(maxsize=2)
def build_attachment_choice_keyboard(script: str = DEFAULT_SCRIPT) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=localize_text_for_script(script, UPLOAD_ADDITIONAL_INFO_BUTTON_TEXT),
                    callback_data="attachment_choice:upload",
                )
            ],
            [
                InlineKeyboardButton(
                    text=localize_text_for_script(script, SKIP_ADDITIONAL_INFO_BUTTON_TEXT),
                    callback_data="attachment_choice:skip",
                )
            ],
        ]
    )


@lru_cache(maxsize=2)
def build_attachment_upload_keyboard(script: str = DEFAULT_SCRIPT) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=localize_text_for_script(script, SKIP_ADDITIONAL_INFO_BUTTON_TEXT),
                    callback_data="attachment_choice:skip",
                )
            ],
        ]
    )


@lru_cache(maxsize=1024)
def build_admin_request_keyboard(
    request_id: int,
    script: str = DEFAULT_SCRIPT,
    allow_reply: bool = True,
    has_attachment: bool = False,
) -> InlineKeyboardMarkup:
    rows = []

    if allow_reply:
        rows.append(
            [
                InlineKeyboardButton(
                    text=localize_text_for_script(script, ADMIN_REPLY_BUTTON_TEXT),
                    callback_data=f"admin_reply:{request_id}",
                )
            ]
        )

    if has_attachment:
        rows.append(
            [
                InlineKeyboardButton(
                    text=localize_text_for_script(script, ADMIN_OPEN_ATTACHMENT_BUTTON_TEXT),
                    callback_data=f"admin_open_attachment:{request_id}",
                )
            ]
        )

    return InlineKeyboardMarkup(inline_keyboard=rows)


@lru_cache(maxsize=2)
def build_help_reply_keyboard(script: str = DEFAULT_SCRIPT) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=localize_text_for_script(script, HELP_SCRIPT_BUTTON_TEXTS["latin"])),
                KeyboardButton(text=localize_text_for_script(script, HELP_SCRIPT_BUTTON_TEXTS["cyrillic"])),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, BACK_TO_MENU_BUTTON_TEXT)),
            ],
        ],
        resize_keyboard=True,
    )


@lru_cache(maxsize=2)
def build_global_admin_keyboard(script: str = DEFAULT_SCRIPT) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=localize_text_for_script(script, GLOBAL_STATS_BUTTON_TEXT)),
            ],
        ],
        resize_keyboard=True,
    )


@lru_cache(maxsize=2)
def build_global_stats_keyboard(script: str = DEFAULT_SCRIPT) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=localize_text_for_script(script, GLOBAL_STATS_RED_BUTTON_TEXT)),
                KeyboardButton(text=localize_text_for_script(script, GLOBAL_STATS_YELLOW_BUTTON_TEXT)),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, GLOBAL_STATS_GREEN_BUTTON_TEXT)),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, BACK_TO_MENU_BUTTON_TEXT)),
            ],
        ],
        resize_keyboard=True,
    )


def normalize_channel_ref(channel: str) -> str:
    value = channel.strip()
    if value.startswith("https://t.me/"):
        value = value.replace("https://t.me/", "", 1)
    elif value.startswith("http://t.me/"):
        value = value.replace("http://t.me/", "", 1)

    value = value.strip("/")
    if value.startswith("@") or value.startswith("-100"):
        return value

    return f"@{value}"


@lru_cache(maxsize=1)
def build_channel_url() -> str:
    channel_ref = normalize_channel_ref(REQUIRED_CHANNEL)
    return f"https://t.me/{channel_ref.lstrip('@')}"


@lru_cache(maxsize=1)
def get_channel_target() -> str:
    if CHANNEL_ID:
        return CHANNEL_ID
    return normalize_channel_ref(REQUIRED_CHANNEL)


@lru_cache(maxsize=2)
def build_main_keyboard(script: str = DEFAULT_SCRIPT) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=localize_text_for_script(script, "\U0001F4E9 Murojaat yuborish")),
                KeyboardButton(text=localize_text_for_script(script, "\U0001F4A1 Taklif berish")),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, "\U0001F4B0 Moliyaviy yordam")),
                KeyboardButton(text=localize_text_for_script(script, "\U0001F4BC Ish o'rinlari")),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, YOUTH_OPPORTUNITIES_SECTION_KEY)),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, CHANGE_REGION_BUTTON_TEXT)),
            ],
        ],
        resize_keyboard=True,
    )


@lru_cache(maxsize=2)
def build_youth_opportunities_keyboard(script: str = DEFAULT_SCRIPT) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=localize_text_for_script(script, "\U0001F4DA Ta'lim")),
                KeyboardButton(text=localize_text_for_script(script, "\U0001F4BC Ish")),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, "\U0001F680 Startap")),
                KeyboardButton(text=localize_text_for_script(script, "\U0001F30D Tanlovlar")),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, "\U0001F9E0 Rivojlanish")),
            ],
            [
                KeyboardButton(text=localize_text_for_script(script, BACK_TO_MENU_BUTTON_TEXT)),
            ],
        ],
        resize_keyboard=True,
    )


@lru_cache(maxsize=2)
def build_back_keyboard(script: str = DEFAULT_SCRIPT) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=localize_text_for_script(script, BACK_TO_MENU_BUTTON_TEXT)),
            ],
        ],
        resize_keyboard=True,
    )


@lru_cache(maxsize=2)
def build_phone_keyboard(script: str = DEFAULT_SCRIPT) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text=localize_text_for_script(script, PHONE_SHARE_BUTTON_TEXT),
                    request_contact=True,
                ),
            ],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


async def is_user_subscribed(bot: Bot, user_id: int, force_refresh: bool = False) -> bool:
    if not force_refresh:
        cached_status = get_cached_subscription_status(user_id)
        if cached_status is not None:
            return cached_status

    channel_target = get_channel_target()

    try:
        member = await bot.get_chat_member(channel_target, user_id)
    except Exception as error:
        logging.exception("Kanal obunasini tekshirishda xato. channel=%s error=%s", channel_target, error)
        cached_status = get_cached_subscription_status(user_id)
        if cached_status is not None:
            return cached_status
        return False

    is_subscribed = member.status in {
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.CREATOR,
    }
    remember_subscription_status(user_id, is_subscribed)
    return is_subscribed


async def clear_inline_message(message: Message | None) -> None:
    if not message:
        return

    try:
        await message.delete()
        return
    except Exception as error:
        logging.info("Inline xabarni o'chirib bo'lmadi: %s", error)

    try:
        await message.edit_reply_markup(reply_markup=None)
    except Exception as error:
        logging.info("Inline tugmalarni olib tashlab bo'lmadi: %s", error)


async def clear_saved_attachment_prompt(
    bot: Bot,
    chat_id: int,
    state: FSMContext,
) -> None:
    data = await state.get_data()
    prompt_message_id = data.get("attachment_prompt_message_id")
    if not prompt_message_id:
        return

    try:
        await bot.delete_message(chat_id=chat_id, message_id=int(prompt_message_id))
    except Exception as error:
        logging.info("Qo'shimcha ma'lumot promptini o'chirib bo'lmadi: %s", error)

    await state.update_data(attachment_prompt_message_id=None)


async def show_section_text_prompt(
    message: Message,
    state: FSMContext,
    section_key: str,
    user_id: int | None = None,
    intro_text: str | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)
    section = SECTION_CONFIG[section_key]
    await clear_saved_attachment_prompt(message.bot, message.chat.id, state)
    await state.set_state(AppealState.waiting_for_section_text)
    await state.update_data(
        active_section=section_key,
        **{UI_SCREEN_KEY: UI_SCREEN_MAIN_MENU},
    )
    text = section["prompt"] if not intro_text else f"{intro_text}\n\n{section['prompt']}"
    await message.answer(
        localize_text_for_user(resolved_user_id, text),
        reply_markup=build_back_keyboard(script),
    )


async def show_optional_attachment_choice(
    message: Message,
    state: FSMContext,
    section_key: str,
    user_id: int | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)
    await state.set_state(AppealState.waiting_for_attachment_choice)
    await state.update_data(
        active_section=section_key,
        attachment_type=None,
        attachment_file_id=None,
        attachment_caption=None,
        attachment_prompt_message_id=None,
        **{UI_SCREEN_KEY: UI_SCREEN_ATTACHMENT_CHOICE},
    )
    await message.answer(
        localize_text_for_user(resolved_user_id, build_optional_attachment_choice_text(section_key)),
        reply_markup=build_attachment_choice_keyboard(script),
    )


async def show_attachment_upload_prompt(
    message: Message,
    state: FSMContext,
    section_key: str,
    user_id: int | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)
    await clear_saved_attachment_prompt(message.bot, message.chat.id, state)
    await state.set_state(AppealState.waiting_for_attachment)
    prompt_message = await message.answer(
        localize_text_for_user(resolved_user_id, build_attachment_upload_text(section_key)),
        reply_markup=build_attachment_upload_keyboard(script),
        parse_mode="Markdown",
    )
    await state.update_data(
        active_section=section_key,
        attachment_type=None,
        attachment_file_id=None,
        attachment_caption=None,
        attachment_prompt_message_id=prompt_message.message_id,
        **{UI_SCREEN_KEY: UI_SCREEN_ATTACHMENT_UPLOAD},
    )


async def show_youth_opportunities_menu(
    message: Message,
    state: FSMContext,
    user_id: int | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)
    await state.clear()
    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_YOUTH_OPPORTUNITIES})
    await message.answer(
        localize_text_for_user(resolved_user_id, build_youth_opportunities_menu_text()),
        reply_markup=build_youth_opportunities_keyboard(script),
    )


async def show_youth_opportunity_info(
    message: Message,
    state: FSMContext,
    opportunity_key: str,
    user_id: int | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)
    opportunity = YOUTH_OPPORTUNITY_CONFIG[opportunity_key]
    await state.clear()
    await state.update_data(
        **{
            UI_SCREEN_KEY: UI_SCREEN_YOUTH_OPPORTUNITY_INFO,
            "youth_opportunity_key": opportunity_key,
        }
    )
    await message.answer(
        localize_text_for_user(resolved_user_id, str(opportunity["text"])),
        reply_markup=build_youth_opportunities_keyboard(script),
    )


async def start_section_flow(
    message: Message,
    state: FSMContext,
    section_key: str,
    user_id: int | None = None,
) -> None:
    await clear_saved_attachment_prompt(message.bot, message.chat.id, state)
    await state.update_data(
        attachment_type=None,
        attachment_file_id=None,
        attachment_caption=None,
        attachment_prompt_message_id=None,
    )
    if supports_optional_attachment(section_key):
        await show_optional_attachment_choice(message, state, section_key, user_id=user_id)
        return

    await show_section_text_prompt(message, state, section_key, user_id=user_id)


async def send_subscription_prompt(
    message: Message,
    state: FSMContext,
    section_key: str | None = None,
    user_id: int | None = None,
    custom_text: str | None = None,
    pending_youth_opportunity_key: str | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)
    await state.clear()
    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_SUBSCRIPTION_PROMPT})
    if section_key:
        await state.update_data(pending_section=section_key)
    if pending_youth_opportunity_key:
        await state.update_data(pending_youth_opportunity_key=pending_youth_opportunity_key)
    text = custom_text or build_subscription_prompt_text()
    await message.answer(
        localize_text_for_user(resolved_user_id, text),
        reply_markup=build_subscription_keyboard(script),
        parse_mode="Markdown",
    )


async def require_subscription_or_start(
    message: Message,
    state: FSMContext,
    bot: Bot,
    section_key: str,
) -> None:
    user_id = message.from_user.id if message.from_user else 0
    script = get_user_script(user_id)
    user = get_user(user_id)

    if not is_user_registered(user):
        await ask_full_name(message, state)
        return

    if not user.get("region"):
        await message.answer(
            localize_text_for_user(user_id, "Iltimos, avval hududingizni tanlang \U0001F447"),
            reply_markup=build_region_keyboard(script),
        )
        return

    if not await is_user_subscribed(bot, user_id):
        await send_subscription_prompt(message, state, section_key, user_id=user_id)
        return

    await start_section_flow(message, state, section_key, user_id=user_id)


async def require_subscription_or_show_youth_content(
    message: Message,
    state: FSMContext,
    bot: Bot,
    opportunity_key: str | None = None,
) -> None:
    user_id = message.from_user.id if message.from_user else 0
    script = get_user_script(user_id)
    user = get_user(user_id)

    if not is_user_registered(user):
        await ask_full_name(message, state)
        return

    if not user.get("region"):
        await message.answer(
            localize_text_for_user(user_id, "Iltimos, avval hududingizni tanlang \U0001F447"),
            reply_markup=build_region_keyboard(script),
        )
        return

    if not await is_user_subscribed(bot, user_id):
        await send_subscription_prompt(
            message,
            state,
            YOUTH_OPPORTUNITIES_SECTION_KEY,
            user_id=user_id,
            pending_youth_opportunity_key=opportunity_key,
        )
        return

    if opportunity_key:
        await show_youth_opportunity_info(message, state, opportunity_key, user_id=user_id)
        return

    await show_youth_opportunities_menu(message, state, user_id=user_id)


async def ask_full_name(message: Message, state: FSMContext, user_id: int | None = None) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)

    if is_staff_user(resolved_user_id):
        await show_staff_home(message, state, user_id=resolved_user_id)
        return

    await state.set_state(RegisterState.waiting_for_full_name)
    await state.update_data(**{UI_SCREEN_KEY: "register_full_name"})
    text = (
        "Assalomu alaykum! \U0001F44B\n\n"
        "\"MUROJAT BOT\"ga xush kelibsiz! \U0001F4E9\n\n"
        "Botdan foydalanish uchun avval ro'yxatdan o'ting.\n\n"
        "Iltimos, ism va familiyangizni yozing.\n"
        "Masalan: Ali Valiyev"
    )
    await message.answer(
        localize_text_for_user(resolved_user_id, text),
        reply_markup=ReplyKeyboardRemove(),
    )


async def ask_phone(message: Message, state: FSMContext, user_id: int | None = None) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)
    await state.set_state(RegisterState.waiting_for_phone)
    await state.update_data(**{UI_SCREEN_KEY: "register_phone"})
    await message.answer(
        localize_text_for_user(
            resolved_user_id,
            "Endi telefon raqamingizni yuboring. \U0001F4F1\n\n"
            "Tugma orqali yuborishingiz yoki qo'lda yozishingiz mumkin.\n"
            "Masalan: +998901234567",
        ),
        reply_markup=build_phone_keyboard(script),
    )


async def ask_region(message: Message, state: FSMContext, user_id: int | None = None) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)

    if is_staff_user(resolved_user_id):
        await show_staff_home(message, state, user_id=resolved_user_id)
        return

    await state.clear()
    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_REGION_SELECTION})
    await message.answer(
        localize_text_for_user(
            resolved_user_id,
            "Rahmat! \U00002705\n\n"
            "Endi Qamashi tumanidagi hududingizni tanlang.\n\n"
            "Quyidagi MFYlardan birini belgilang \U0001F447",
        ),
        reply_markup=build_region_keyboard(script),
    )


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(DEFAULT_BOT_COMMANDS)


async def apply_user_commands(bot: Bot, user_id: int) -> None:
    script = get_user_script(user_id)
    await bot.set_my_commands(
        build_localized_bot_commands(script=script, is_admin=is_primary_admin(user_id)),
        scope=BotCommandScopeChat(chat_id=user_id),
    )


async def apply_user_commands_safely(bot: Bot, user_id: int) -> None:
    try:
        await apply_user_commands(bot, user_id)
    except Exception as error:
        logging.warning("Foydalanuvchi komandalarini yangilab bo'lmadi. user_id=%s error=%s", user_id, error)


@lru_cache(maxsize=2)
def build_help_text(is_admin: bool = False) -> str:
    lines = [
        "Yordamchi komandalar:",
        "",
        "1. /start - botni qayta ishga tushiradi",
        "2. /help - komandalar ro'yxatini ko'rsatadi",
        "3. /menu - asosiy menyuni ochadi",
        "4. /region - hududni qayta tanlashni boshlaydi",
        "5. /cancel - joriy amalni bekor qiladi",
    ]

    if is_admin:
        lines.append("6. /admin - admin panelni ochadi")

    return "\n".join(lines)


async def show_staff_home(
    message: Message,
    state: FSMContext | None = None,
    user_id: int | None = None,
    include_help_keyboard: bool = False,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    script = get_user_script(resolved_user_id)

    if not is_staff_user(resolved_user_id):
        return

    if resolved_user_id:
        asyncio.create_task(apply_user_commands_safely(message.bot, resolved_user_id))

    if state is not None:
        await state.clear()
        await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_STAFF_HOME})

    if is_global_admin(resolved_user_id):
        reply_markup = build_global_admin_keyboard(script)
    else:
        reply_markup = build_help_reply_keyboard(script) if include_help_keyboard else ReplyKeyboardRemove()

    await message.answer(
        localize_text_for_user(resolved_user_id, build_staff_home_text(resolved_user_id)),
        reply_markup=reply_markup,
    )


async def show_global_stats_dashboard(
    message: Message,
    state: FSMContext | None = None,
    user_id: int | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    if not is_global_admin(resolved_user_id):
        await show_staff_home(message, state, user_id=resolved_user_id)
        return

    script = get_user_script(resolved_user_id)
    region_counts = get_unanswered_request_counts_by_region()

    if state is not None:
        await state.clear()
        await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_GLOBAL_STATS, "global_stats_signal": None})

    await message.answer(
        localize_text_for_user(resolved_user_id, build_global_stats_text(region_counts)),
        reply_markup=build_global_stats_keyboard(script),
    )


async def show_global_stats_group(
    message: Message,
    state: FSMContext | None,
    signal_name: str,
    user_id: int | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id)
    if not is_global_admin(resolved_user_id):
        await show_staff_home(message, state, user_id=resolved_user_id)
        return

    script = get_user_script(resolved_user_id)
    region_counts = get_unanswered_request_counts_by_region()

    if state is not None:
        await state.clear()
        await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_GLOBAL_STATS, "global_stats_signal": signal_name})

    await message.answer(
        localize_text_for_user(resolved_user_id, build_global_stats_group_text(signal_name, region_counts)),
        reply_markup=build_global_stats_keyboard(script),
    )


async def show_main_menu(
    message: Message,
    state: FSMContext,
    user_id: int | None = None,
    *,
    check_subscription: bool = False,
    intro_text: str | None = None,
) -> None:
    resolved_user_id = extract_message_user_id(message, user_id) or 0
    script = get_user_script(resolved_user_id)

    if is_staff_user(resolved_user_id):
        await show_staff_home(message, state, user_id=resolved_user_id)
        return

    user = get_user(resolved_user_id)
    if not is_user_registered(user):
        await ask_full_name(message, state, user_id=resolved_user_id)
        return

    selected_region = user.get("region")
    if not selected_region:
        await ask_region(message, state, user_id=resolved_user_id)
        return

    if check_subscription and not await is_user_subscribed(message.bot, resolved_user_id):
        text = (
            "Assalomu alaykum! \U0001F44B\n\n"
            f"Sizning tanlangan hududingiz: {selected_region} \U0001F4CD\n\n"
            "Endi botdan foydalanish uchun kanalga qo'shilishingiz kerak.\n\n"
            "Kanalga a'zo bo'lgach, `Tekshirish` tugmasini bosing."
        )
        await send_subscription_prompt(message, state, user_id=resolved_user_id, custom_text=text)
        return

    await state.clear()
    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_MAIN_MENU})
    text = intro_text or "Kerakli bo'limni tanlang \U0001F447"
    await message.answer(
        localize_text_for_user(resolved_user_id, text),
        reply_markup=build_main_keyboard(script),
    )


async def redirect_staff_callback(call: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_STAFF_HOME})
    await call.answer()
    await clear_inline_message(call.message)
    await call.message.answer(
        localize_text_for_user(call.from_user.id, build_staff_home_text(call.from_user.id)),
        reply_markup=ReplyKeyboardRemove(),
    )


async def show_home(message: Message, state: FSMContext) -> None:
    await state.clear()
    user_id = message.from_user.id if message.from_user else 0
    if user_id:
        asyncio.create_task(apply_user_commands_safely(message.bot, user_id))
    user = get_user(user_id) or {}
    selected_region = user.get("region")
    intro_text = (
        "Assalomu alaykum! \U0001F44B\n\n"
        f"Sizning tanlangan hududingiz: {selected_region} \U0001F4CD\n\n"
        "\"MUROJAT BOT\"ga xush kelibsiz! \U0001F4E9\n\n"
        "Kerakli bo'limni tanlang \U0001F447"
        if selected_region
        else None
    )
    await show_main_menu(
        message,
        state,
        user_id=user_id,
        check_subscription=True,
        intro_text=intro_text,
    )


async def show_region_selection(message: Message, state: FSMContext) -> None:
    await state.clear()
    user_id = message.from_user.id if message.from_user else 0
    script = get_user_script(user_id)

    if is_staff_user(user_id):
        await show_staff_home(message, state)
        return

    if not is_user_registered(get_user(user_id)):
        await ask_full_name(message, state)
        return

    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_REGION_SELECTION})
    await message.answer(
        localize_text_for_user(user_id, "Hududni qaytadan tanlang \U0001F447"),
        reply_markup=build_region_keyboard(script),
    )


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    await show_home(message, state)


@dp.message(Command("help"))
async def help_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id if message.from_user else None
    script = get_user_script(user_id)

    if is_staff_user(user_id):
        reply_markup = build_global_admin_keyboard(script) if is_global_admin(user_id) else build_help_reply_keyboard(script)
        await message.answer(
            localize_text_for_user(user_id, build_staff_home_text(user_id)),
            reply_markup=reply_markup,
        )
        return

    is_admin = is_primary_admin(user_id)
    await message.answer(
        localize_text_for_user(user_id, build_help_text(is_admin=is_admin)),
        reply_markup=build_help_reply_keyboard(script),
    )


@dp.callback_query(lambda call: call.data and call.data.startswith("help_script:"))
async def help_script_handler(call: CallbackQuery, state: FSMContext) -> None:
    user_id = call.from_user.id if call.from_user else None

    if not call.message or not getattr(call.message, "text", None):
        await call.answer()
        return

    script = call.data.split(":", maxsplit=1)[1]
    if script not in SUPPORTED_SCRIPTS:
        await call.answer(localize_text_for_user(user_id, "Noma'lum yordam tili"), show_alert=True)
        return

    previous_script = get_user_script(user_id)
    script_changed = previous_script != script

    if script_changed:
        set_user_script(user_id, script)
        asyncio.create_task(apply_user_commands_safely(call.bot, user_id))

    await call.answer(localize_text_for_user(user_id, "Yozuv o'zgartirildi."))
    await clear_inline_message(call.message)
    await sync_ui_after_script_change(call.message, state, user_id)


@dp.message(lambda message: resolve_script_choice(message.text) is not None)
async def script_switch_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id if message.from_user else None
    script = resolve_script_choice(message.text)

    if not user_id or not script:
        return

    if get_user_script(user_id) != script:
        set_user_script(user_id, script)
        asyncio.create_task(apply_user_commands_safely(message.bot, user_id))

    await sync_ui_after_script_change(message, state, user_id)


@dp.message(Command("menu"))
async def menu_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await show_main_menu(message, state, intro_text="Kerakli bo'limni tanlang \U0001F447")


@dp.message(Command("region"))
async def region_command_handler(message: Message, state: FSMContext) -> None:
    await show_region_selection(message, state)


@dp.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext) -> None:
    had_state = bool(await state.get_state())
    await state.clear()
    user_id = message.from_user.id if message.from_user else None

    if had_state:
        await message.answer(
            localize_text_for_user(user_id, "Joriy amal bekor qilindi. \U00002705"),
            reply_markup=ReplyKeyboardRemove(),
        )

    await show_home(message, state)


@dp.message(Command("admin"))
async def admin_panel_handler(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if not message.from_user or not is_primary_admin(message.from_user.id):
        await message.answer(localize_text_for_user(user_id, "Bu bo'lim faqat admin uchun. \U0001F512"))
        return

    managed_regions = get_admin_scope_regions(user_id)
    stats = get_admin_stats(user_id)
    latest_requests = get_latest_requests(user_id=user_id)
    if managed_regions is None:
        managed_regions_text = "Barcha MFYlar"
    else:
        managed_regions_text = ", ".join(managed_regions) if managed_regions else "Hozircha biriktirilgan MFY yo'q."

    text = (
        "\U0001F4CA Admin panel\n\n"
        f"\U0001F4CD Sizga biriktirilgan MFYlar: {managed_regions_text}\n\n"
        f"\U0001F465 Foydalanuvchilar: {stats['users_count']} ta\n"
        f"\U0001F4E5 Murojaatlar: {stats['requests_count']} ta\n\n"
        "\U0001F552 Oxirgi murojaatlar:\n"
    )

    if not latest_requests:
        text += "Hozircha sizga biriktirilgan murojaatlar yo'q."
    else:
        for request in latest_requests:
            preview = request["message"].replace("\n", " ")
            if len(preview) > 80:
                preview = preview[:80] + "..."

            text += (
                f"\n#{request['id']} | {request['section']}\n"
                f"\U0001F4CD {request['region']}\n"
                f"\U0001F464 {request['full_name']} | \U0001F4F1 {request['phone']}\n"
                f"\U0001F4DD {preview}\n"
            )

        text += f"\n{build_admin_reply_help_text()}"

    await message.answer(localize_text_for_user(user_id, text))


@dp.message(lambda message: message.from_user and is_global_admin(message.from_user.id) and matches_localized_text(message.text, GLOBAL_STATS_BUTTON_TEXT))
async def global_stats_button_handler(message: Message, state: FSMContext) -> None:
    await show_global_stats_dashboard(message, state)


@dp.message(lambda message: message.from_user and is_global_admin(message.from_user.id) and resolve_global_stats_signal(message.text) is not None)
async def global_stats_group_handler(message: Message, state: FSMContext) -> None:
    signal_name = resolve_global_stats_signal(message.text)
    if not signal_name:
        return

    await show_global_stats_group(message, state, signal_name)


@dp.message(lambda message: matches_localized_text(message.text, BACK_TO_MENU_BUTTON_TEXT))
async def main_menu_button_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await show_main_menu(
        message,
        state,
        intro_text="Asosiy menyu ochildi. Kerakli bo'limni tanlang \U0001F447",
    )

@dp.message(lambda message: matches_localized_text(message.text, CHANGE_REGION_BUTTON_TEXT))
async def change_region_handler(message: Message, state: FSMContext) -> None:
    await show_region_selection(message, state)


@dp.message(RegisterState.waiting_for_full_name)
async def full_name_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id if message.from_user else 0

    if is_staff_user(user_id):
        await show_staff_home(message, state)
        return

    full_name = (message.text or "").strip()

    if len(full_name.split()) < 2:
        await message.answer(
            localize_text_for_user(
                user_id,
                "Iltimos, ism va familiyangizni to'liq yozing.\n"
                "Masalan: Ali Valiyev",
            )
        )
        return

    await state.update_data(full_name=full_name)
    await ask_phone(message, state, user_id=user_id)


@dp.message(RegisterState.waiting_for_phone)
async def phone_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id if message.from_user else 0

    if is_staff_user(user_id):
        await show_staff_home(message, state)
        return

    phone = message.contact.phone_number if message.contact else (message.text or "").strip()

    if not phone:
        await message.answer(localize_text_for_user(user_id, "Iltimos, telefon raqamingizni yuboring."))
        return

    data = await state.get_data()
    full_name = data.get("full_name")

    if not full_name:
        await ask_full_name(message, state)
        return

    username = message.from_user.username if message.from_user else None
    save_user_profile(user_id, full_name, phone, username)
    await ask_region(message, state, user_id=user_id)

@dp.message(lambda message: bool(resolve_section_key(message.text)))
async def section_menu_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    user_id = message.from_user.id if message.from_user else None

    if is_staff_user(user_id):
        await show_staff_home(message, state)
        return

    section_key = resolve_section_key(message.text)
    if not section_key:
        return

    if section_key == YOUTH_OPPORTUNITIES_SECTION_KEY:
        await require_subscription_or_show_youth_content(message, state, bot)
        return

    await require_subscription_or_start(message, state, bot, section_key)


@dp.message(lambda message: bool(resolve_youth_opportunity_key(message.text)))
async def youth_opportunity_menu_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    user_id = message.from_user.id if message.from_user else None

    if is_staff_user(user_id):
        await show_staff_home(message, state)
        return

    opportunity_key = resolve_youth_opportunity_key(message.text)
    if not opportunity_key:
        return

    await require_subscription_or_show_youth_content(message, state, bot, opportunity_key)


@dp.callback_query(lambda call: call.data == "check_subscription")
async def check_subscription_handler(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    user_id = call.from_user.id

    if is_staff_user(user_id):
        await redirect_staff_callback(call, state)
        return

    if not await is_user_subscribed(bot, user_id, force_refresh=True):
        await call.answer(localize_text_for_user(user_id, "Siz hali kanalga qo'shilmagansiz"), show_alert=True)
        return

    data = await state.get_data()
    pending_section = data.get("pending_section")
    pending_youth_opportunity_key = data.get("pending_youth_opportunity_key")
    await state.clear()
    await call.answer()
    await clear_inline_message(call.message)

    if pending_section == YOUTH_OPPORTUNITIES_SECTION_KEY:
        if pending_youth_opportunity_key in YOUTH_OPPORTUNITY_CONFIG:
            await show_youth_opportunity_info(call.message, state, str(pending_youth_opportunity_key), user_id=user_id)
            return

        await show_youth_opportunities_menu(call.message, state, user_id=user_id)
        return

    if pending_section:
        await start_section_flow(call.message, state, pending_section, user_id=user_id)
    else:
        await show_home(call.message, state)


@dp.callback_query(lambda call: call.data and call.data.startswith("attachment_choice:"))
async def attachment_choice_handler(call: CallbackQuery, state: FSMContext) -> None:
    user_id = call.from_user.id

    if is_staff_user(user_id):
        await redirect_staff_callback(call, state)
        return

    current_state = await state.get_state()
    if current_state not in {
        AppealState.waiting_for_attachment_choice.state,
        AppealState.waiting_for_attachment.state,
    }:
        await call.answer(localize_text_for_user(user_id, "Bu tugma eskirgan. \U0001F504"), show_alert=True)
        return

    data = await state.get_data()
    section_key = data.get("active_section", "\U0001F4E9 Murojaat yuborish")
    action = call.data.split(":", maxsplit=1)[1]

    await call.answer()
    await clear_inline_message(call.message)
    await state.update_data(attachment_prompt_message_id=None)

    if action == "upload":
        await show_attachment_upload_prompt(call.message, state, section_key, user_id=user_id)
        return

    await show_section_text_prompt(call.message, state, section_key, user_id=user_id)


@dp.message(AppealState.waiting_for_attachment_choice)
async def attachment_choice_message_handler(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    await message.answer(
        localize_text_for_user(
            user_id,
            "Iltimos, pastdagi tugmalardan birini tanlang \U0001F447",
        )
    )


@dp.message(AppealState.waiting_for_attachment)
async def attachment_upload_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id if message.from_user else 0

    if is_staff_user(user_id):
        await show_staff_home(message, state)
        return

    attachment_type = None
    attachment_file_id = None

    if message.photo:
        attachment_type = "photo"
        attachment_file_id = message.photo[-1].file_id
    elif message.video:
        attachment_type = "video"
        attachment_file_id = message.video.file_id
    elif message.document:
        attachment_type = "document"
        attachment_file_id = message.document.file_id

    if not attachment_type or not attachment_file_id:
        prompt_message = await message.answer(
            localize_text_for_user(
                user_id,
                "Iltimos, qo'shimcha ma'lumot sifatida foto, video yoki hujjat yuboring. \U0001F4CE",
            ),
            reply_markup=build_attachment_upload_keyboard(get_user_script(user_id)),
        )
        await state.update_data(attachment_prompt_message_id=prompt_message.message_id)
        return

    await state.update_data(
        attachment_type=attachment_type,
        attachment_file_id=attachment_file_id,
        attachment_caption=(message.caption or "").strip() or None,
    )
    data = await state.get_data()
    section_key = data.get("active_section", "\U0001F4E9 Murojaat yuborish")
    await show_section_text_prompt(
        message,
        state,
        section_key,
        user_id=user_id,
        intro_text=build_attachment_saved_notice(attachment_type),
    )


@dp.message(AppealState.waiting_for_section_text)
async def save_appeal_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    user_id = message.from_user.id if message.from_user else 0
    user_script = get_user_script(user_id)

    if is_staff_user(user_id):
        await show_staff_home(message, state)
        return

    user = get_user(user_id) or {}
    full_name = user.get("full_name") or (message.from_user.full_name if message.from_user else "Noma'lum")
    phone = user.get("phone", "Kiritilmagan")
    username = f"@{message.from_user.username}" if message.from_user and message.from_user.username else "mavjud emas"
    region = user.get("region", "Tanlanmagan")
    appeal_text = message.text or ""
    data = await state.get_data()
    section_key = data.get("active_section", "\U0001F4E9 Murojaat yuborish")
    section = SECTION_CONFIG.get(section_key, SECTION_CONFIG["\U0001F4E9 Murojaat yuborish"])
    attachment_type = data.get("attachment_type")
    attachment_file_id = data.get("attachment_file_id")
    attachment_caption = data.get("attachment_caption")

    if not appeal_text.strip():
        await message.answer(localize_text_for_user(user_id, "Iltimos, xabaringizni matn ko'rinishida yuboring."))
        return

    request_id = save_request(
        user_id=user_id,
        section=section["title"],
        region=region,
        staff_user_id=resolve_staff_user_id_for_region(region),
        full_name=full_name,
        phone=phone,
        username=username,
        message=build_message_with_attachment_note(appeal_text, attachment_type),
        attachment_type=attachment_type,
        attachment_file_id=attachment_file_id,
    )

    admin_text = (
        f"\U0001F4E5 Yangi ma'lumot keldi #{request_id}\n\n"
        f"\U0001F4C2 Bo'lim: {section['title']}\n"
        f"\U0001F4CD MFY: {region}\n"
        f"\U0001F464 Foydalanuvchi: {full_name}\n"
        f"\U0001F4F1 Telefon: {phone}\n"
        f"\U0001F194 Telegram ID: {user_id}\n"
        f"\U0001F517 Username: {username}\n\n"
        f"\U00002753 Unga nima kerak:\n{appeal_text}"
        f"{build_attachment_admin_note(attachment_type, attachment_caption)}"
    )

    await state.clear()
    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_MAIN_MENU})
    await message.answer(
        localize_text_for_user(user_id, section["success"]),
        reply_markup=build_main_keyboard(user_script),
    )
    asyncio.create_task(
        notify_staff_about_request(
            bot,
            request_id,
            admin_text,
            resolve_staff_user_id_for_region(region),
            attachment_type,
            attachment_file_id,
        )
    )


@dp.message(lambda message: message.from_user and is_region_admin(message.from_user.id) and message.reply_to_message)
async def admin_reply_handler(message: Message, bot: Bot) -> None:
    user_id = message.from_user.id if message.from_user else None
    request_data = get_request_by_admin_message(message.reply_to_message.message_id)
    if not request_data:
        fallback_request_id = parse_request_id_from_admin_text(
            getattr(message.reply_to_message, "text", None) or getattr(message.reply_to_message, "caption", None)
        )
        request_data = get_request_by_id(fallback_request_id) if fallback_request_id is not None else None

    if not request_data or not can_staff_reply_to_request(user_id, request_data):
        return

    if not message.text:
        await message.answer(localize_text_for_user(user_id, "Iltimos, foydalanuvchiga javobni matn ko'rinishida yozing."))
        return

    response_text = await send_admin_reply_to_user(bot, request_data, message.text, answered_by=user_id)
    await message.answer(localize_text_for_user(user_id, response_text))


@dp.callback_query(lambda call: call.data and call.data.startswith("admin_reply:"))
async def admin_reply_button_handler(call: CallbackQuery, state: FSMContext) -> None:
    if not is_region_admin(call.from_user.id):
        await call.answer(localize_text_for_user(call.from_user.id, "Bu tugma faqat admin uchun"), show_alert=True)
        return

    request_id = int(call.data.split(":", maxsplit=1)[1])
    started = await start_admin_reply_by_request_id(call.message, state, request_id, user_id=call.from_user.id)
    if not started:
        await call.answer(localize_text_for_user(call.from_user.id, "Murojaat topilmadi"), show_alert=True)
        return

    await call.answer()


@dp.callback_query(lambda call: call.data and call.data.startswith("admin_open_attachment:"))
async def admin_open_attachment_handler(call: CallbackQuery) -> None:
    user_id = call.from_user.id
    if not is_staff_user(user_id):
        await call.answer(localize_text_for_user(user_id, "Bu tugma faqat admin uchun"), show_alert=True)
        return

    request_id = int(call.data.split(":", maxsplit=1)[1])
    request_data = get_request_by_id(request_id)

    if not request_data or not can_staff_view_request(user_id, request_data):
        await call.answer(localize_text_for_user(user_id, "Bu fayl sizga biriktirilmagan"), show_alert=True)
        return

    attachment_type = request_data.get("attachment_type")
    attachment_file_id = request_data.get("attachment_file_id")
    if not attachment_type or not attachment_file_id:
        await call.answer(localize_text_for_user(user_id, "Qo'shimcha fayl topilmadi"), show_alert=True)
        return

    await call.answer(localize_text_for_user(user_id, "Qo'shimcha fayl yuborilmoqda..."))
    try:
        await send_request_attachment_on_demand(
            call.bot,
            user_id,
            request_data,
            str(attachment_type),
            str(attachment_file_id),
        )
    except Exception as error:
        logging.warning("Qo'shimcha faylni yuborib bo'lmadi. request_id=%s user_id=%s error=%s", request_id, user_id, error)
        await call.message.answer(localize_text_for_user(user_id, "Qo'shimcha faylni yuborib bo'lmadi."))


@dp.message(AdminReplyState.waiting_for_reply)
async def admin_reply_text_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    user_id = message.from_user.id if message.from_user else None
    if not message.from_user or not is_region_admin(message.from_user.id):
        await message.answer(localize_text_for_user(user_id, "Bu amal faqat admin uchun. \U0001F512"))
        return

    if not message.text:
        await message.answer(localize_text_for_user(user_id, "Iltimos, javobni matn ko'rinishida yozing."))
        return

    data = await state.get_data()
    request_id = data.get("reply_request_id")
    request_data = get_request_by_id(int(request_id)) if request_id else None

    selected_request_id = parse_request_shortcut(message.text)
    if selected_request_id is not None:
        await start_admin_reply_by_request_id(message, state, selected_request_id, user_id=user_id)
        return

    if not request_data:
        await state.clear()
        await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_STAFF_HOME})
        await message.answer(localize_text_for_user(user_id, "Murojaat topilmadi."))
        return

    if not can_staff_reply_to_request(user_id, request_data):
        await state.clear()
        await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_STAFF_HOME})
        await message.answer(localize_text_for_user(user_id, "Bu murojaat sizga biriktirilmagan."))
        return

    response_text = await send_admin_reply_to_user(bot, request_data, message.text, answered_by=user_id)
    await state.clear()
    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_STAFF_HOME})
    await message.answer(localize_text_for_user(user_id, response_text))


@dp.message(lambda message: message.from_user and is_region_admin(message.from_user.id) and not message.reply_to_message)
async def admin_plain_message_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id if message.from_user else None
    request_id = parse_request_shortcut(message.text or "")

    if request_id is None:
        await message.answer(localize_text_for_user(user_id, build_admin_reply_help_text()))
        return

    await start_admin_reply_by_request_id(message, state, request_id, user_id=user_id)


@dp.callback_query(lambda call: call.data and call.data.startswith("region:"))
async def region_handler(call: CallbackQuery, state: FSMContext) -> None:
    user_id = call.from_user.id
    script = get_user_script(user_id)

    if is_staff_user(user_id):
        await redirect_staff_callback(call, state)
        return

    region_code = call.data.split(":", maxsplit=1)[1]
    selected_region = REGION_CODES.get(region_code)

    if not selected_region:
        await call.answer(localize_text_for_user(user_id, "Hudud topilmadi"), show_alert=True)
        return

    user = get_user(user_id)
    if not is_user_registered(user):
        await call.answer()
        await clear_inline_message(call.message)
        await ask_full_name(call.message, state, user_id=user_id)
        return

    update_user_region(user_id, selected_region)
    await call.answer()
    await clear_inline_message(call.message)

    if await is_user_subscribed(call.bot, user_id):
        await state.clear()
        await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_MAIN_MENU})
        text = (
            f"Hududingiz yangilandi: {selected_region} \U00002705\n\n"
            "Kerakli bo'limni tanlang \U0001F447"
        )
        await call.message.answer(localize_text_for_user(user_id, text), reply_markup=build_main_keyboard(script))
        return

    text = (
        f"Hududingiz qabul qilindi: {selected_region} \U00002705\n\n"
        "Endi botdan foydalanishni boshlash uchun rasmiy kanalga qo'shiling. \U0001F4E2\n\n"
        "Kanalga a'zo bo'lgach, `Tekshirish` tugmasini bosing."
    )
    await send_subscription_prompt(call.message, state, user_id=user_id, custom_text=text)


@dp.message(lambda message: (message.from_user and not (get_user(message.from_user.id) or {}).get("region")))
async def ask_region_first_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id if message.from_user else 0
    script = get_user_script(user_id)

    if is_staff_user(user_id):
        await show_staff_home(message, state)
        return

    if not is_user_registered(get_user(user_id)):
        await ask_full_name(message, state)
        return

    await state.clear()
    await state.update_data(**{UI_SCREEN_KEY: UI_SCREEN_REGION_SELECTION})
    await message.answer(
        localize_text_for_user(
            user_id,
            "Hudud tanlashdan oldin boshqa bo'limlardan foydalanib bo'lmaydi. \U0001F6AB\n\n"
            "Iltimos, avval Qamashi tumanidagi hududingizni tanlang \U0001F447",
        ),
        reply_markup=build_region_keyboard(script),
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    init_db()
    logging.info("MFYlar uchun %s ta alohida admin biriktirildi.", len(REGION_LEADER_IDS))
    bot = Bot(token=BOT_TOKEN)
    await setup_bot_commands(bot)
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        if mongo_client is not None:
            mongo_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot to'xtatildi")
