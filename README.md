# MUROJAT BOT

Telegram orqali Qamashi tumani yoshlari murojaatlarini qabul qiluvchi bot.

## Imkoniyatlar

- Foydalanuvchidan ism-familiya, telefon raqam va MFYni oladi
- Kanalga obunani tekshiradi
- Murojaat, taklif, moliyaviy yordam, ish o'rinlari va yoshlar imkoniyatlari bo'yicha so'rov qabul qiladi
- `/help` oynasida lotincha va kirillcha ko'rinishni almashtirish mumkin
- Ma'lumotlarni MongoDB'ga saqlaydi, ulanish bo'lmasa avtomatik SQLite fallback ishlaydi
- Botdagi har bir MFY uchun alohida admin-yetakchi biriktiriladi
- Foydalanuvchi tanlagan MFY bo'yicha murojaat faqat o'sha MFY adminiga yuboriladi
- Har bir admin faqat o'ziga biriktirilgan bitta MFY murojaatlariga javob qaytara oladi
- `ADMIN_ID` glavniy boshliq sifatida barcha MFYlardan kelgan murojaatlarni ko'radi
- Kuzatuvchi odamga ma'lumot boradi, lekin u javob bera olmaydi
- `/admin` orqali qisqa statistika ko'rish mumkin

## O'rnatish

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows uchun:

```powershell
py -m venv .venv
.\.venv\Scripts\activate
py -m pip install -r requirements.txt
```

## Sozlash

`.env.example` fayldan `.env` yarating va qiymatlarni to'ldiring:

```env
BOT_TOKEN=your_bot_token_here
ADMIN_ID=123456789
ADMIN_URL=@glavniy_nazoratchi
REQUIRED_CHANNEL=@your_channel_username
CHANNEL_ID=-1001234567890
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB_NAME=murojat_bot

# Azlartepa
MFY_ADMIN_AZLARTEPA=111111111 | @azlartepa_admin
# Badaxshon
MFY_ADMIN_BADAXSHON=222222222 | @badaxshon_admin
# Boburtepa
MFY_ADMIN_BOBURTEPA=333333333 | @boburtepa_admin
```

`ADMIN_ID` glavniy boshliq hisoblanadi. U barcha MFYlardan kelgan murojaatlarni ko'radi va /admin orqali umumiy nazorat qiladi.
`ADMIN_URL` esa glavniy kuzatuvchining Telegram manzili uchun.

Har bir MFY uchun `.env` ichida alohida qator bo'ladi. Format:

```text
MFY_ADMIN_<MFY_NOMI>=TELEGRAM_ID | @username
```

Masalan:

```text
MFY_ADMIN_AZLARTEPA=111111111 | @azlartepa_admin
MFY_ADMIN_UZUN=222222222 | @uzun_admin
```

Bu yerda:

- `111111111` bu adminning Telegram ID si va bot uchun asosiy qiymat shu
- `@username` adashmaslik uchun yoziladigan Telegram manzil

Fuqaro `Azlartepa`ni tanlasa, murojaat `MFY_ADMIN_AZLARTEPA` ichidagi Telegram ID egasiga yuboriladi va javobni ham faqat shu yetakchi bera oladi.

Muhim:

- Botdagi barcha MFYlar uchun `MFY_ADMIN_*` qatorlari bo'lishi shart
- Bitta Telegram ID faqat bitta MFYga biriktirilishi kerak
- Biror MFY qoldirilsa yoki bitta admin bir nechta MFYga yozilsa, bot ishga tushmaydi
- Kanal bo'yicha obuna tekshiruvi bitta umumiy kanal orqali ishlaydi, har bir MFY uchun alohida kanal kerak emas

`MONGODB_URI` orqali local MongoDB yoki MongoDB Atlas ulanish manzilini berasiz.

## Ishga tushirish

```bash
python main.py
```

Windows uchun:

```powershell
py main.py
```

## Database

Bot odatda MongoDB ichida `murojat_bot` nomli database bilan ishlaydi. Nomni `MONGODB_DB_NAME` orqali o'zgartirishingiz mumkin.

Collectionlar:

- `users` - foydalanuvchilar
- `requests` - murojaatlar
- `counters` - murojaat ID hisoblagichi
- `meta` - xizmat ma'lumotlari

Agar MongoDB ulanmasa, bot avtomatik `murojat_bot.db` SQLite fallback bilan ishga tushadi.

Agar eski `murojat_bot.db` SQLite fayli mavjud bo'lsa va MongoDB bo'sh bo'lsa, bot birinchi ishga tushishda ma'lumotlarni avtomatik ko'chiradi.

## MongoDB ni Ishga Tushirish

Local MongoDB uchun misol:

```powershell
docker run -d --name murojat-mongo -p 27017:27017 mongo:7
```

So'ng `.env` ichida quyidagini ishlatishingiz mumkin:

```env
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB_NAME=murojat_bot
```

## VPSda 24/7 Ishlatish

1. Loyihani serverga yuklang
2. Python virtual environment yarating
3. MongoDB ishlayotganini tekshiring
4. `pip install -r requirements.txt` qiling
5. `.env` faylni to'ldiring
6. `deploy/murojat-bot.service.example` faylini server yo'lingizga moslang
7. `systemd` orqali ishga tushiring

Misol:

```bash
sudo cp deploy/murojat-bot.service.example /etc/systemd/system/murojat-bot.service
sudo systemctl daemon-reload
sudo systemctl enable murojat-bot
sudo systemctl start murojat-bot
sudo systemctl status murojat-bot
```
