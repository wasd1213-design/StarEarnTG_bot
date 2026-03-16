import os
import asyncio
from datetime import datetime, timedelta, timezone

import psycopg2
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    WebAppInfo,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("MY_DATABASE_URL")

ADMINS = [514167463]
BOT_USERNAME_FOR_REFLINK = "StarEarnTG_bot"
WEBAPP_URL = os.getenv("WEBAPP_URL")

IS_ACTIVE = True

START_BONUS = 20
WEEKLY_HOLD_BONUS = 20
MAX_WEEKLY_HOLD_BONUSES = 4
EXTRA_SPIN_COST = 1

PREMIUM_COST = 1300
WITHDRAW_MIN = 700
CHANNEL_PROMO_COST = 200
CHANNEL_PROMO_PRIORITY_COST = 250

FAQ_ITEMS = {
    "start": {
        "title": "🔰 Начало работы",
        "text": (
            "❓ <b>Как начать пользоваться ботом?</b>\n"
            "• Нажмите /start или кнопку «🌠 Звёздное Колесо» в меню\n"
            "• Подпишитесь на всех активных спонсоров (отмечены в профиле)\n"
            "• Пригласите 2 активных реферала\n"
            "• После выполнения условий доступ к Колесу активируется автоматически ✅\n\n"
            "❓ <b>Что такое активный реферал?</b>\n"
            "Реферал считается активным, если он:\n"
            "• Перешёл по вашей реферальной ссылке\n"
            "• Подписался на все основные спонсорские каналы (слоты 1 и 2)\n\n"
            "⚠️ Временный спонсор (слот 3) не учитывается при проверке активности рефералов.\n\n"
            "❓ <b>Где моя реферальная ссылка?</b>\n"
            "Откройте «👤 Профиль» — внизу будет ваша персональная ссылка.\n"
            "Копируйте и делитесь с друзьями!"
        ),
    },
    "wheel": {
        "title": "🌠 Звёздное Колесо",
        "text": (
            "❓ <b>Как крутить Колесо?</b>\n"
            f"• 🎁 Бесплатно: 1 раз в 6 часов\n"
            f"• ⭐ За звёзды: дополнительное вращение за {EXTRA_SPIN_COST} ⭐\n"
            "• Откройте через кнопку «🌠 Звёздное Колесо» в меню или WebApp\n\n"
            "❓ <b>Почему Колесо недоступно?</b>\n"
            "Возможные причины:\n"
            "• ❌ Не подписаны на спонсоров — подпишитесь на все каналы из списка\n"
            "• ❌ Мало активных рефералов — пригласите ещё друзей\n"
            "• ⏳ Кулдаун 6 часов — подождите или купите доп. спин\n\n"
            "❓ <b>Как проверить статус подписок?</b>\n"
            "Нажмите «🔄 Обновить статус» под главным сообщением или введите /start."
        ),
    },
    "levels": {
        "title": "🏅 Уровни и бонусы",
        "text": (
            "❓ <b>Как работают уровни?</b>\n\n"
            "🥉 <b>Bronze</b>\n"
            "• Активных рефералов: 0–4\n"
            "• Бонус к выигрышу: +0%\n"
            "• Следующий уровень: Silver (5)\n\n"
            "🥈 <b>Silver</b>\n"
            "• Активных рефералов: 5–9\n"
            "• Бонус к выигрышу: +15%\n"
            "• Следующий уровень: Gold (10)\n\n"
            "🥇 <b>Gold</b>\n"
            "• Активных рефералов: 10–14\n"
            "• Бонус к выигрышу: +35%\n"
            "• Следующий уровень: Diamond (15)\n\n"
            "💎 <b>Diamond</b>\n"
            "• Активных рефералов: 15+\n"
            "• Бонус к выигрышу: +60%\n"
            "• Максимальный уровень\n\n"
            "🎯 Бонус применяется к выигрышам в Колесе.\n\n"
            "❓ <b>Как получить уведомление о повышении уровня?</b>\n"
            "Бот автоматически уведомит вас при повышении уровня.\n"
            "Если пропустили — откройте «👤 Профиль»."
        ),
    },
    "weekly": {
        "title": "🎁 Еженедельный бонус за подписку",
        "text": (
            "❓ <b>Что это за бонус?</b>\n"
            "За сохранение подписки на 2 основных спонсора вы можете получать:\n"
            f"• 💰 <b>{WEEKLY_HOLD_BONUS} ⭐</b> в неделю\n"
            f"• 📅 Максимум <b>{MAX_WEEKLY_HOLD_BONUSES} недели</b> "
            f"(всего <b>{WEEKLY_HOLD_BONUS * MAX_WEEKLY_HOLD_BONUSES} ⭐</b>)\n\n"
            "❓ <b>Почему мне не начислили бонус?</b>\n"
            "Возможные причины:\n"
            "• ❌ Отписались от одного из основных спонсоров\n"
            "• ⏳ Прошло меньше 7 дней с последнего начисления\n\n"
            "⚠️ Временный спонсор (слот 3) не влияет на этот бонус."
        ),
    },
    "exchange": {
        "title": "💱 Обмен звёзд",
        "text": (
            "❓ <b>На что можно потратить звёзды?</b>\n"
            f"• 💎 Telegram Premium (3 мес) — {PREMIUM_COST} ⭐\n"
            f"• 💸 Вывод звёзд — от {WITHDRAW_MIN} ⭐\n"
            f"• 📢 Канал в списке спонсоров — {CHANNEL_PROMO_COST} ⭐\n"
            f"• ⚡ Вне очереди (приоритет) — {CHANNEL_PROMO_PRIORITY_COST} ⭐\n\n"
            "❓ <b>Как оформить заявку?</b>\n"
            "• Откройте «🔄 Обмен звёзд»\n"
            "• Выберите нужную опцию\n"
            "• Подтвердите списание\n"
            "• Заявка автоматически уйдёт администратору\n\n"
            "❓ <b>Как вывести звёзды?</b>\n"
            "Только для Diamond-пользователей:\n"
            f"• Минимальная сумма: {WITHDRAW_MIN} ⭐\n"
            "• Оформите заявку в меню «Обмен звёзд»\n"
            "• Администратор обработает запрос вручную\n"
            "• Звёзды будут переведены на ваш аккаунт"
        ),
    },
    "sponsors": {
        "title": "📢 Спонсорская программа",
        "text": (
            "❓ <b>Как разместить свой канал?</b>\n"
            "• Откройте «🔄 Обмен звёзд» → «📢 Ваш канал в списке спонсоров»\n"
            f"• Оплатите размещение: {CHANNEL_PROMO_COST} ⭐ (обычный) "
            f"или {CHANNEL_PROMO_PRIORITY_COST} ⭐ (вне очереди)\n"
            "• Отправьте боту username канала (например: @mychannel)\n"
            "• Убедитесь, что @StarEarnTG_bot добавлен в администраторы канала\n\n"
            "❓ <b>Как работает очередь?</b>\n"
            "• 🎯 Цель: 100 подписчиков на ваш канал\n"
            "• 📊 Бот отслеживает, кто подписался и остался в канале\n"
            "• ✅ При достижении цели заказ помечается как выполненный\n"
            "• 🔁 Освободившийся слот занимает следующий заказ из очереди\n\n"
            "❓ <b>Что такое временный слот?</b>\n"
            "• Слот №3 — дополнительный, для быстрых размещений\n"
            "• Не влияет на активацию Колеса и недельные бонусы\n"
            "• Освобождается сразу после выполнения заказа"
        ),
    },
    "profile": {
        "title": "👤 Профиль и настройки",
        "text": (
            "❓ <b>Что отображается в профиле?</b>\n"
            "• 🪪 Имя, username, ID\n"
            "• ⭐ Баланс звёзд\n"
            "• 🏅 Текущий уровень и бонус\n"
            "• 🎁 Количество полученных недельных бонусов\n"
            "• 👥 Количество активных рефералов\n"
            "• 🔗 Ваша реферальная ссылка\n"
            "• 🏆 Лидерборд"
        ),
    },
    "leaderboard": {
        "title": "🏆 Лидерборд",
        "text": (
            "❓ <b>Как попасть в топ?</b>\n"
            "Лидерборд ранжирует пользователей по балансу звёзд:\n"
            "• Откройте «🏆 Лидерборд» в меню\n"
            "• Топ-10 отображаются с указанием имени и количества звёзд\n"
            "• Обновляется в реальном времени\n\n"
            "💡 Совет: активно приглашайте рефералов и участвуйте в активностях, "
            "чтобы увеличить баланс!"
        ),
    },
    "problems": {
        "title": "❓ Частые проблемы",
        "text": (
            "❌ <b>Недостаточно звёзд</b>\n"
            "• Проверьте баланс в «👤 Профиль»\n"
            "• Заработайте больше: крутите Колесо, приглашайте друзей, получайте недельные бонусы\n\n"
            "❌ <b>Бот не является администратором канала</b>\n"
            "• Добавьте @StarEarnTG_bot в администраторы вашего канала\n"
            "• Дайте права: «Публикация сообщений» + «Приглашение пользователей»\n"
            "• Повторите отправку username\n\n"
            "❌ <b>Звёздное Колесо недоступно</b>\n"
            "• Проверьте подписки на спонсоров\n"
            "• Убедитесь, что у вас ≥ 2 активных реферала\n"
            "• Нажмите «🔄 Обновить статус»"
        ),
    },
}


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_naive_utc(dt):
    if dt is None:
        return None
    return dt.replace(tzinfo=None)


def display_username(username: str) -> str:
    if not username:
        return "Без ника"
    return f"@{username.lstrip('@')}"


def normalize_channel_username(channel: str) -> str:
    if not channel:
        return ""
    channel = channel.strip()
    if channel.startswith("https://t.me/"):
        channel = channel.replace("https://t.me/", "@")
    if channel.startswith("http://t.me/"):
        channel = channel.replace("http://t.me/", "@")
    if not channel.startswith("@"):
        channel = f"@{channel.lstrip('@')}"
    return channel


def get_faq_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔰 Начало работы", callback_data="faq:start")],
            [InlineKeyboardButton("🌠 Звёздное Колесо", callback_data="faq:wheel")],
            [InlineKeyboardButton("🏅 Уровни и бонусы", callback_data="faq:levels")],
            [InlineKeyboardButton("🎁 Еженедельный бонус", callback_data="faq:weekly")],
            [InlineKeyboardButton("💱 Обмен звёзд", callback_data="faq:exchange")],
            [InlineKeyboardButton("📢 Спонсорская программа", callback_data="faq:sponsors")],
            [InlineKeyboardButton("👤 Профиль", callback_data="faq:profile")],
            [InlineKeyboardButton("🏆 Лидерборд", callback_data="faq:leaderboard")],
            [InlineKeyboardButton("❓ Частые проблемы", callback_data="faq:problems")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")],
        ]
    )


def build_faq_text(selected_key=None):
    text = (
        "📚 <b>FAQ — Звёздное Колесо</b>\n"
        "Полное руководство по использованию бота.\n"
        "Сохраните себе или поделитесь с рефералами!"
    )

    if selected_key and selected_key in FAQ_ITEMS:
        item = FAQ_ITEMS[selected_key]
        text += f"\n\n{item['title']}\n\n{item['text']}"

    return text


async def notify_admins(context: ContextTypes.DEFAULT_TYPE, text: str):
    for admin_id in ADMINS:
        try:
            await context.bot.send_message(admin_id, text, parse_mode=ParseMode.HTML)
        except Exception as e:
            print("notify_admins error:", e)


async def check_subscription(user_id, channel, context):
    try:
        member = await context.bot.get_chat_member(chat_id=channel, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False


def get_level_info(ref_count: int):
    if ref_count >= 15:
        return {
            "name": "Diamond",
            "emoji": "💎",
            "bonus_percent": 60,
            "next_target": None,
            "next_name": None,
        }
    if ref_count >= 10:
        return {
            "name": "Gold",
            "emoji": "🥇",
            "bonus_percent": 35,
            "next_target": 15,
            "next_name": "Diamond",
        }
    if ref_count >= 5:
        return {
            "name": "Silver",
            "emoji": "🥈",
            "bonus_percent": 15,
            "next_target": 10,
            "next_name": "Gold",
        }
    return {
        "name": "Bronze",
        "emoji": "🥉",
        "bonus_percent": 0,
        "next_target": 5,
        "next_name": "Silver",
    }


def get_reply_menu(user_id: int, bonus_percent: int = 0):
    return ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(
                    f"🌠 Звёздное Колесо (+{bonus_percent}% к выигрышу)",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL}?user_id={user_id}&v=2"),
                ),
            ],
            [
                KeyboardButton("👤 Профиль"),
                KeyboardButton("🔄 Обмен звёзд"),
            ],
            [
                KeyboardButton("🏆 Лидерборд"),
                KeyboardButton("📚 FAQ"),
            ],
        ],
        resize_keyboard=True,
    )


def get_main_inline():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить статус", callback_data="check_sub")]
    ])


def get_exchange_inline():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"💎 Telegram Premium 3 мес — {PREMIUM_COST} ⭐", callback_data="exchange_premium")],
            [InlineKeyboardButton("💸 Вывод звёзд", callback_data="exchange_withdraw")],
            [InlineKeyboardButton(f"📢 Ваш канал в списке спонсоров — {CHANNEL_PROMO_COST} ⭐", callback_data="exchange_promo")],
            [InlineKeyboardButton(f"⚡ Вне очереди — {CHANNEL_PROMO_PRIORITY_COST} ⭐", callback_data="exchange_promo_priority")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")],
        ]
    )


def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        referrer_id BIGINT NULL,
                        activated BOOLEAN DEFAULT FALSE,
                        all_subscribed INT DEFAULT 0,
                        tickets INT DEFAULT 0,
                        last_fortune_time TIMESTAMP NULL,
                        lifetime_ref_count INT DEFAULT 0,
                        weekly_hold_bonus_count INT DEFAULT 0,
                        last_hold_bonus_at TIMESTAMP NULL,
                        last_level_notified TEXT DEFAULT 'Bronze',
                        last_seen TIMESTAMP NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS referrals (
                        referrer_id BIGINT,
                        referred_id BIGINT,
                        is_valid BOOLEAN DEFAULT FALSE,
                        checked_at TIMESTAMP NULL,
                        UNIQUE(referrer_id, referred_id)
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS channel_subscriptions (
                        user_id BIGINT,
                        channel_id TEXT,
                        subscribed_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (user_id, channel_id)
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fortune_spins (
                        spin_id TEXT PRIMARY KEY,
                        user_id BIGINT,
                        prize_code TEXT,
                        created_at TIMESTAMP
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS exchange_requests (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        username TEXT,
                        exchange_type TEXT,
                        stars_amount INT,
                        status TEXT DEFAULT 'new',
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sponsor_slots (
                        slot_no INT PRIMARY KEY,
                        sponsor_type TEXT NOT NULL,
                        channel_username TEXT,
                        order_id INT NULL,
                        is_active BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sponsor_orders (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        username TEXT,
                        channel_username TEXT,
                        target_subscribers INT DEFAULT 100,
                        counted_subscribers INT DEFAULT 0,
                        active_subscribers INT DEFAULT 0,
                        priority_level INT DEFAULT 0,
                        stars_amount INT NOT NULL,
                        status TEXT DEFAULT 'waiting_link',
                        placed_in_slot BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW(),
                        completed_at TIMESTAMP NULL
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sponsor_order_members (
                        order_id INT NOT NULL,
                        user_id BIGINT NOT NULL,
                        counted_at TIMESTAMP DEFAULT NOW(),
                        still_subscribed BOOLEAN DEFAULT TRUE,
                        PRIMARY KEY (order_id, user_id)
                    )
                    """
                )

                alter_statements = [
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS activated BOOLEAN DEFAULT FALSE",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS all_subscribed INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS tickets INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_fortune_time TIMESTAMP NULL",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_active_at TIMESTAMP NULL",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS lifetime_ref_count INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS weekly_hold_bonus_count INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_hold_bonus_at TIMESTAMP NULL",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_level_notified TEXT DEFAULT 'Bronze'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP NULL",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id BIGINT NULL",
                ]

                for stmt in alter_statements:
                    try:
                        cursor.execute(stmt)
                    except Exception as e:
                        print("ALTER warning:", e)

                try:
                    cursor.execute("ALTER TABLE users DROP COLUMN IF EXISTS profile_badge")
                except Exception as e:
                    print("DROP COLUMN warning:", e)

                cursor.execute(
                    """
                    INSERT INTO sponsor_slots (slot_no, sponsor_type, is_active)
                    VALUES
                        (1, 'main', FALSE),
                        (2, 'main', FALSE),
                        (3, 'temp', FALSE)
                    ON CONFLICT (slot_no) DO NOTHING
                    """
                )

                conn.commit()

        print("✅ База данных подключена и инициализирована.")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")


def get_active_sponsors_sync(include_temp=True):
    sponsors = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if include_temp:
                cur.execute(
                    """
                    SELECT slot_no, sponsor_type, channel_username
                    FROM sponsor_slots
                    WHERE is_active = TRUE AND channel_username IS NOT NULL
                    ORDER BY slot_no
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT slot_no, sponsor_type, channel_username
                    FROM sponsor_slots
                    WHERE is_active = TRUE
                      AND sponsor_type = 'main'
                      AND channel_username IS NOT NULL
                    ORDER BY slot_no
                    """
                )
            rows = cur.fetchall()

    for slot_no, sponsor_type, channel_username in rows:
        sponsors.append(
            {
                "slot_no": slot_no,
                "sponsor_type": sponsor_type,
                "channel_username": channel_username,
            }
        )
    return sponsors


async def get_active_sponsors(include_temp=True):
    return get_active_sponsors_sync(include_temp=include_temp)


async def recount_temp_order_progress(context: ContextTypes.DEFAULT_TYPE):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT order_id, channel_username
                    FROM sponsor_slots
                    WHERE slot_no = 3
                      AND is_active = TRUE
                      AND order_id IS NOT NULL
                      AND channel_username IS NOT NULL
                    """
                )
                row = cur.fetchone()
                if not row:
                    return

                order_id, channel_username = row

                cur.execute("SELECT user_id FROM users")
                users = [r[0] for r in cur.fetchall()]

                for user_id in users:
                    is_sub = await check_subscription(user_id, channel_username, context)
                    if is_sub:
                        cur.execute(
                            """
                            INSERT INTO sponsor_order_members (order_id, user_id, counted_at, still_subscribed)
                            VALUES (%s, %s, %s, TRUE)
                            ON CONFLICT (order_id, user_id)
                            DO UPDATE SET still_subscribed = TRUE
                            """,
                            (order_id, user_id, utcnow()),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE sponsor_order_members
                            SET still_subscribed = FALSE
                            WHERE order_id = %s AND user_id = %s
                            """,
                            (order_id, user_id),
                        )

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM sponsor_order_members
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )
                counted_total = int(cur.fetchone()[0] or 0)

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM sponsor_order_members
                    WHERE order_id = %s AND still_subscribed = TRUE
                    """,
                    (order_id,),
                )
                active_total = int(cur.fetchone()[0] or 0)

                cur.execute(
                    """
                    UPDATE sponsor_orders
                    SET counted_subscribers = %s,
                        active_subscribers = %s
                    WHERE id = %s
                    RETURNING target_subscribers, user_id, channel_username
                    """,
                    (counted_total, active_total, order_id),
                )
                order_row = cur.fetchone()
                if not order_row:
                    conn.commit()
                    return

                target_subscribers, order_user_id, order_channel = order_row

                if counted_total >= int(target_subscribers):
                    cur.execute(
                        """
                        UPDATE sponsor_orders
                        SET status = 'completed',
                            completed_at = %s
                        WHERE id = %s
                        """,
                        (utcnow(), order_id),
                    )
                    cur.execute(
                        """
                        UPDATE sponsor_slots
                        SET channel_username = NULL,
                            order_id = NULL,
                            is_active = FALSE
                        WHERE slot_no = 3
                        """
                    )
                    conn.commit()

                    await notify_admins(
                        context,
                        (
                            f"✅ <b>Временный спонсорский заказ выполнен</b>\n\n"
                            f"Заказ #{order_id}\n"
                            f"Канал: <b>{order_channel}</b>\n"
                            f"Привлечено: <b>{counted_total}/{target_subscribers}</b>"
                        ),
                    )

                    try:
                        await context.bot.send_message(
                            chat_id=order_user_id,
                            text=(
                                f"✅ <b>Ваш заказ выполнен</b>\n\n"
                                f"Канал: <b>{order_channel}</b>\n"
                                f"Привлечено: <b>{counted_total}/{target_subscribers}</b>"
                            ),
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception as e:
                        print("notify order user error:", e)

                    await place_next_temp_order(context)
                    return

                conn.commit()
    except Exception as e:
        print("recount_temp_order_progress error:", e)


async def place_next_temp_order(context: ContextTypes.DEFAULT_TYPE):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT is_active FROM sponsor_slots WHERE slot_no = 3")
                row = cur.fetchone()
                if row and row[0]:
                    return

                cur.execute(
                    """
                    SELECT id, channel_username
                    FROM sponsor_orders
                    WHERE status = 'approved'
                      AND placed_in_slot = FALSE
                      AND channel_username IS NOT NULL
                    ORDER BY priority_level DESC, created_at ASC
                    LIMIT 1
                    """
                )
                order = cur.fetchone()
                if not order:
                    return

                order_id, channel_username = order

                cur.execute(
                    """
                    UPDATE sponsor_slots
                    SET channel_username = %s,
                        order_id = %s,
                        is_active = TRUE
                    WHERE slot_no = 3
                    """,
                    (channel_username, order_id),
                )

                cur.execute(
                    """
                    UPDATE sponsor_orders
                    SET placed_in_slot = TRUE,
                        status = 'active'
                    WHERE id = %s
                    """,
                    (order_id,),
                )
                conn.commit()

                await notify_admins(
                    context,
                    (
                        f"📢 <b>В слот 3 поставлен новый временный спонсор</b>\n\n"
                        f"Заказ #{order_id}\n"
                        f"Канал: <b>{channel_username}</b>"
                    ),
                )
    except Exception as e:
        print("place_next_temp_order error:", e)


async def count_valid_refs(referrer_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    valid_count = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT referred_id, COALESCE(is_valid, FALSE) FROM referrals WHERE referrer_id=%s",
                (referrer_id,),
            )
            rows = cur.fetchall()

            main_sponsors = await get_active_sponsors(include_temp=False)

            for referred_id, is_valid in rows:
                if is_valid:
                    valid_count += 1
                    continue

                if len(main_sponsors) < 2:
                    continue

                subscribed_all_main = True
                for sponsor in main_sponsors:
                    if not await check_subscription(referred_id, sponsor["channel_username"], context):
                        subscribed_all_main = False
                        break

                if subscribed_all_main:
                    cur.execute(
                        """
                        UPDATE referrals
                        SET is_valid = TRUE, checked_at = %s
                        WHERE referrer_id = %s AND referred_id = %s
                        """,
                        (utcnow(), referrer_id, referred_id),
                    )
                    valid_count += 1

            cur.execute(
                "UPDATE users SET lifetime_ref_count = %s WHERE user_id = %s",
                (valid_count, referrer_id),
            )

            if valid_count >= 2:
                cur.execute(
                    "UPDATE users SET activated = TRUE WHERE user_id = %s",
                    (referrer_id,),
                )

            conn.commit()

    return valid_count


async def get_user_state(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    sponsors = await get_active_sponsors(include_temp=True)

    all_subs_ok = True
    channels_list = ""

    if not sponsors:
        channels_list = "Список спонсоров пока не настроен.\n"
        all_subs_ok = False

    for sponsor in sponsors:
        ch = sponsor["channel_username"]
        is_sub = await check_subscription(user_id, ch, context)

        if is_sub:
            icon = "✅"
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO channel_subscriptions (user_id, channel_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (user_id, ch),
                        )
                        conn.commit()
            except Exception as e:
                print("channel_subscriptions save error:", e)
        else:
            icon = "❌"
            all_subs_ok = False

        channels_list += f"• {ch} {icon}\n"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET all_subscribed = %s WHERE user_id = %s",
                (1 if all_subs_ok else 0, user_id),
            )
            cur.execute(
                """
                SELECT
                    COALESCE(activated, FALSE),
                    COALESCE(lifetime_ref_count, 0),
                    COALESCE(tickets, 0),
                    COALESCE(weekly_hold_bonus_count, 0),
                    last_fortune_time,
                    COALESCE(last_level_notified, 'Bronze')
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            conn.commit()

    activated = False
    ref_count = 0
    stars = 0
    weekly_hold_bonus_count = 0
    last_fortune_time = None
    last_level_notified = "Bronze"

    if row:
        (
            activated,
            ref_count,
            stars,
            weekly_hold_bonus_count,
            last_fortune_time,
            last_level_notified,
        ) = row

    level = get_level_info(ref_count)

    return {
        "activated": activated,
        "all_subs_ok": all_subs_ok,
        "channels_list": channels_list,
        "ref_count": ref_count,
        "stars": stars,
        "weekly_hold_bonus_count": weekly_hold_bonus_count,
        "last_fortune_time": to_naive_utc(last_fortune_time),
        "level": level,
        "last_level_notified": last_level_notified,
    }


async def notify_level_up_if_needed(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        state = await get_user_state(user_id, context)
        current_level = state["level"]["name"]

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(last_level_notified, 'Bronze') FROM users WHERE user_id = %s",
                    (user_id,),
                )
                row = cur.fetchone()
                prev_level = row[0] if row else "Bronze"

                if prev_level != current_level:
                    cur.execute(
                        "UPDATE users SET last_level_notified = %s WHERE user_id = %s",
                        (current_level, user_id),
                    )
                    conn.commit()

                    await context.bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"🎉 <b>Поздравляем!</b>\n\n"
                            f"Ваш уровень повышен до <b>{state['level']['emoji']} {current_level}</b>\n"
                            f"🌠 <b>Бонус к выигрышу:</b> +{state['level']['bonus_percent']}%"
                        ),
                        parse_mode=ParseMode.HTML,
                    )
    except Exception as e:
        print("notify_level_up_if_needed error:", e)



async def apply_inactivity_decay(user_id: int, context):
    now = utcnow()

    def _apply():
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(tickets, 0), last_active_at
                    FROM users
                    WHERE user_id = %s
                """, (user_id,))
                row = cur.fetchone()

                if not row:
                    return {"decayed": 0, "new_balance": None}

                tickets, last_active_at = row
                tickets = int(tickets or 0)

                if last_active_at is None:
                    cur.execute("""
                        UPDATE users
                        SET last_active_at = %s
                        WHERE user_id = %s
                    """, (now, user_id))
                    conn.commit()
                    return {"decayed": 0, "new_balance": tickets}

                inactive_days = (now - last_active_at).days
                decayed = 0
                new_balance = tickets

                if inactive_days > 7 and tickets > 100:
                    penalty_days = inactive_days - 7
                    penalty = penalty_days * 2
                    new_balance = max(100, tickets - penalty)
                    decayed = tickets - new_balance

                    cur.execute("""
                        UPDATE users
                        SET tickets = %s,
                            last_active_at = %s
                        WHERE user_id = %s
                    """, (new_balance, now, user_id))
                else:
                    cur.execute("""
                        UPDATE users
                        SET last_active_at = %s
                        WHERE user_id = %s
                    """, (now, user_id))

                conn.commit()
                return {"decayed": decayed, "new_balance": new_balance}

    return await asyncio.to_thread(_apply)


async def get_start_text(user_id, first_name, context):
    state = await get_user_state(user_id, context)

    activation_text = (
        "✅ <b>Доступ к колесу активирован</b>\n"
        if state["activated"]
        else "⚠️ <b>Для открытия колеса пригласите 2 активных реферала</b>\n"
    )

    if not state["all_subs_ok"]:
        wheel_access = "❌ <b>Звёздное Колесо недоступно: подпишитесь на всех активных спонсоров</b>"
    elif not state["activated"]:
        wheel_access = "❌ <b>Звёздное Колесо недоступно: не хватает 2 активных рефералов</b>"
    else:
        wheel_access = "✅ <b>Звёздное Колесо доступно</b>"

    if state["level"]["next_target"]:
        left = max(0, state["level"]["next_target"] - state["ref_count"])
        progress_text = (
            f"📈 До уровня <b>{state['level']['next_name']}</b>: "
            f"<b>{state['ref_count']}/{state['level']['next_target']}</b> "
            f"(осталось {left})\n"
        )
    else:
        progress_text = "👑 У вас максимальный уровень\n"

    return (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        f"🌠 <b>Добро пожаловать в Звёздное Колесо</b>\n\n"
        f"{activation_text}"
        f"{wheel_access}\n\n"
        f"⭐ <b>Ваш баланс:</b> {state['stars']}\n"
        f"🏅 <b>Ваш уровень:</b> {state['level']['emoji']} {state['level']['name']}\n"
        f"🎯 <b>Бонус к выигрышу:</b> +{state['level']['bonus_percent']}%\n\n"
        f"📈 <b>Как быстрее расти в боте:</b>\n"
        f"• Колесо даёт базовый заработок\n"
        f"• Основной прирост звёзд идёт через активных рефералов\n"
        f"• Чем больше активных рефералов — тем выше ваш уровень\n"
        f"• Чем выше уровень — тем больше выигрыш с каждого вращения\n\n"
        f"👥 <b>Активные рефералы:</b> {state['ref_count']}\n"
        f"{progress_text}\n"
        f"💎 <b>Самый сильный прирост даёт уровень Diamond</b> — "
        f"там максимальный бонус к выигрышу.\n\n"
        f"📌 <b>Активные спонсоры:</b>\n{state['channels_list']}"
    )


async def process_weekly_hold_bonus(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    main_sponsors = await get_active_sponsors(include_temp=False)

    if len(main_sponsors) < 2:
        return False, "Основные спонсоры не настроены"

    for sponsor in main_sponsors:
        if not await check_subscription(user_id, sponsor["channel_username"], context):
            return False, "Нет подписки на одного из основных спонсоров"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(weekly_hold_bonus_count, 0),
                    last_hold_bonus_at
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()

            if not row:
                return False, "Пользователь не найден"

            weekly_count = int(row[0] or 0)
            last_hold_bonus_at = to_naive_utc(row[1])

            if weekly_count >= MAX_WEEKLY_HOLD_BONUSES:
                return False, "Достигнут максимум недельных бонусов"

            now = utcnow()
            if last_hold_bonus_at and (now - last_hold_bonus_at) < timedelta(days=7):
                return False, "Бонус уже начислялся меньше недели назад"

            cur.execute(
                """
                UPDATE users
                SET tickets = tickets + %s,
                    weekly_hold_bonus_count = weekly_hold_bonus_count + 1,
                    last_hold_bonus_at = %s
                WHERE user_id = %s
                """,
                (WEEKLY_HOLD_BONUS, now, user_id),
            )
            conn.commit()

    return True, f"Начислено {WEEKLY_HOLD_BONUS} ⭐"


async def show_profile(query_or_update, user_id: int, first_name: str, context: ContextTypes.DEFAULT_TYPE, edit=False):
    decay_result = await apply_inactivity_decay(user_id, context)
    state = await get_user_state(user_id, context)
    me = query_or_update.from_user if hasattr(query_or_update, "from_user") else query_or_update.effective_user

    reflink = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"

    ref_count = state["ref_count"]
    level_name = state["level"]["name"]
    bonus_percent = state["level"]["bonus_percent"]

    if level_name == "Bronze":
        refs_range = "0–4 реферала"
        next_hint = f"📈 До Silver осталось: <b>{max(0, 5 - ref_count)}</b>"
        benefit_hint = "На Silver шанс выигрыша уже выше благодаря бонусу <b>+15%</b>."
    elif level_name == "Silver":
        refs_range = "5–9 рефералов"
        next_hint = f"📈 До Gold осталось: <b>{max(0, 10 - ref_count)}</b>"
        benefit_hint = "На Gold шанс выигрыша становится ещё лучше: бонус <b>+35%</b>."
    elif level_name == "Gold":
        refs_range = "10–14 рефералов"
        next_hint = f"📈 До Diamond осталось: <b>{max(0, 15 - ref_count)}</b>"
        benefit_hint = "💎 На Diamond шанс выигрыша заметно выше: максимальный бонус <b>+60%</b>."
    else:
        refs_range = "15+ рефералов"
        next_hint = "👑 У вас максимальный уровень."
        benefit_hint = "💎 На Diamond у вас максимальный бонус к выигрышу — <b>+60%</b>."

    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"Имя: <b>{first_name}</b>\n"
        f"Username: <b>{display_username(me.username)}</b>\n"
        f"ID: <code>{user_id}</code>\n\n"
        f"⭐ Баланс: <b>{state['stars']}</b>\n"
        f"🏅 Уровень: <b>{state['level']['emoji']} {level_name}</b>\n"
        f"👥 Диапазон уровня: <b>{refs_range}</b>\n"
        f"🎯 Бонус к выигрышу: <b>+{bonus_percent}%</b>\n"
        f"{next_hint}\n"
        f"{benefit_hint}\n\n"
        f"🎁 Недельных бонусов получено: <b>{state['weekly_hold_bonus_count']}/{MAX_WEEKLY_HOLD_BONUSES}</b>\n\n"
        f"👥 Активные рефералы: <b>{ref_count}</b>\n"
        f"🔗 Ваша реферальная ссылка:\n<code>{reflink}</code>"
    )

    if decay_result.get("decayed", 0) > 0:
        text += (
            f"\n\n⚠️ <b>За период неактивности было списано:</b> {decay_result['decayed']}⭐\n"
            f"Чтобы сохранять баланс, заходите в бот хотя бы 1 раз в 7 дней."
        )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])

    if edit:
        await query_or_update.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        await query_or_update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

async def faq_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    selected_key = None
    if query.data.startswith("faq:"):
        selected_key = query.data.split(":", 1)[1]

    try:
        await query.edit_message_text(
            build_faq_text(selected_key),
            parse_mode=ParseMode.HTML,
            reply_markup=get_faq_keyboard(),
        )
    except Exception as e:
        if "not modified" in str(e).lower():
            pass
        else:
            raise e


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not IS_ACTIVE:
        await update.message.reply_text("⛔️ Бот временно на паузе.")
        return

    user = update.effective_user
    user_id = user.id
    username = user.username
    first_name = user.first_name or "друг"
    decay_result = await apply_inactivity_decay(user_id, context)

    referrer_id = None
    if context.args:
        try:
            referrer_id = int(context.args[0])
            if referrer_id == user_id:
                referrer_id = None
        except Exception:
            referrer_id = None

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
            exists = cur.fetchone()

            if not exists:
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, first_name, referrer_id, tickets, last_seen)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (user_id, username, first_name, referrer_id, START_BONUS, utcnow()),
                )

                if referrer_id:
                    cur.execute(
                        """
                        INSERT INTO referrals (referrer_id, referred_id, is_valid, checked_at)
                        VALUES (%s, %s, FALSE, NULL)
                        ON CONFLICT (referrer_id, referred_id) DO NOTHING
                        """,
                        (referrer_id, user_id),
                    )
            else:
                cur.execute(
                    """
                    UPDATE users
                    SET username = %s,
                        first_name = %s,
                        last_seen = %s
                    WHERE user_id = %s
                    """,
                    (username, first_name, utcnow(), user_id),
                )

            conn.commit()

    await count_valid_refs(user_id, context)

    if referrer_id:
        await count_valid_refs(referrer_id, context)
        await notify_level_up_if_needed(referrer_id, context)

    text = await get_start_text(user_id, first_name, context)


    if decay_result.get("decayed", 0) > 0:
        text += (
            f"\n\n⚠️ <b>За период неактивности было списано:</b> "
            f"{decay_result['decayed']}⭐\n"
            f"Чтобы сохранять баланс, заходите в бот хотя бы 1 раз в 7 дней."
        )
    state = await get_user_state(user_id, context)

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_inline(),
    )
    await update.message.reply_text(
        "Выберите действие из меню ниже 👇",
        reply_markup=get_reply_menu(user_id, state["level"]["bonus_percent"]),
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        if not IS_ACTIVE:
            await query.edit_message_text("⛔️ Бот временно на паузе.", parse_mode=ParseMode.HTML)
            return

        uid = query.from_user.id
        data = query.data

        if data in ("check_sub", "back_to_main"):
            decay_result = await apply_inactivity_decay(uid, context)
            await count_valid_refs(uid, context)
            await recount_temp_order_progress(context)
            text = await get_start_text(uid, query.from_user.first_name, context)

            if decay_result.get("decayed", 0) > 0:
                text += (
                    f"\n\n⚠️ <b>За период неактивности было списано:</b> {decay_result['decayed']}⭐\n"
                    f"Чтобы сохранять баланс, заходите в бот хотя бы 1 раз в 7 дней."
                )

            try:
                await query.edit_message_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_inline(),
                )
            except Exception as e:
                if "not modified" in str(e).lower():
                    pass
                else:
                    raise e

        elif data == "profile":
            await show_profile(query, uid, query.from_user.first_name, context, edit=True)

        elif data == "exchange":
            state = await get_user_state(uid, context)
            text = (
                f"🔄 <b>Обмен звёзд</b>\n"
                f"⭐ Ваш баланс: <b>{state['stars']}</b>\n"
                f"Выберите нужное действие:"
            )
            try:
                await query.edit_message_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_exchange_inline(),
                )
            except Exception as e:
                if "not modified" in str(e).lower():
                    pass
                else:
                    raise e

        elif data == "exchange_premium":
            state = await get_user_state(uid, context)

            if state["stars"] < PREMIUM_COST:
                await query.edit_message_text(
                    "❌ <b>Недостаточно звёзд</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            if state["level"]["name"] != "Diamond":
                await query.edit_message_text(
                    "❌ <b>Доступно только для Diamond-уровня Звёздного Колеса!</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (PREMIUM_COST, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (uid, query.from_user.username, "premium_3m", PREMIUM_COST),
                    )
                    conn.commit()

            await notify_admins(
                context,
                (
                    f"💎 <b>Новая заявка на Telegram Premium 3 мес</b>\n\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\n"
                    f"🆔 ID: <code>{uid}</code>\n"
                    f"⭐ Списано: <b>{PREMIUM_COST}</b>\n"
                    f"💰 Остаток: <b>{new_balance}</b>"
                ),
            )

            await query.edit_message_text(
                "✅ Заявка на Telegram Premium создана. Администратор получил уведомление.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        elif data == "exchange_withdraw":
            state = await get_user_state(uid, context)

            if state["stars"] < WITHDRAW_MIN:
                await query.edit_message_text(
                    f"❌ <b>Недостаточно звёзд</b>\nМинимум для вывода: <b>{WITHDRAW_MIN} ⭐</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            if state["level"]["name"] != "Diamond":
                await query.edit_message_text(
                    "❌ <b>Доступно только для Diamond-уровня Звёздного Колеса!</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (WITHDRAW_MIN, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (uid, query.from_user.username, "withdraw", WITHDRAW_MIN),
                    )
                    conn.commit()

            await notify_admins(
                context,
                (
                    f"💸 <b>Новая заявка на вывод звёзд</b>\n\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\n"
                    f"🆔 ID: <code>{uid}</code>\n"
                    f"⭐ Списано: <b>{WITHDRAW_MIN}</b>\n"
                    f"💰 Остаток: <b>{new_balance}</b>"
                ),
            )

            await query.edit_message_text(
                "✅ Заявка на вывод создана. Администратор получил уведомление.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        elif data in ("exchange_promo", "exchange_promo_priority"):
            stars_cost = CHANNEL_PROMO_COST if data == "exchange_promo" else CHANNEL_PROMO_PRIORITY_COST
            priority_level = 0 if data == "exchange_promo" else 1

            state = await get_user_state(uid, context)
            if state["stars"] < stars_cost:
                await query.edit_message_text(
                    "❌ <b>Недостаточно звёзд</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (stars_cost, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO sponsor_orders (
                            user_id, username, target_subscribers, counted_subscribers,
                            active_subscribers, priority_level, stars_amount, status, placed_in_slot
                        )
                        VALUES (%s, %s, %s, 0, 0, %s, %s, 'waiting_link', FALSE)
                        RETURNING id
                        """,
                        (uid, query.from_user.username, 100, priority_level, stars_cost),
                    )
                    order_id = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            uid,
                            query.from_user.username,
                            "sponsor_channel_priority" if priority_level == 1 else "sponsor_channel",
                            stars_cost,
                        ),
                    )
                    conn.commit()

            context.user_data["waiting_sponsor_order_id"] = order_id

            await notify_admins(
                context,
                (
                    f"📢 <b>Новая заявка на размещение канала</b>\n\n"
                    f"Заказ #{order_id}\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\n"
                    f"🆔 ID: <code>{uid}</code>\n"
                    f"⭐ Списано: <b>{stars_cost}</b>\n"
                    f"💰 Остаток: <b>{new_balance}</b>\n"
                    f"Тип: <b>{'вне очереди' if priority_level == 1 else 'обычный'}</b>\n\n"
                    f"Пользователь должен прислать username канала."
                ),
            )

            await query.edit_message_text(
                "✅ Заявка создана.\n\n"
                "Теперь отправьте <b>username канала</b> одним сообщением.\n"
                "Пример: <code>@mychannel</code>\n\n"
                "⚠️ Бот должен быть добавлен администратором в канал.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        else:
            await query.edit_message_text("Неизвестное действие.")

    except Exception as e:
        try:
            await query.edit_message_text(f"Ошибка: {e}")
        except Exception:
            pass


async def text_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not IS_ACTIVE:
        await update.message.reply_text("⛔️ Бот временно на паузе.")
        return

    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    user = update.effective_user
    uid = user.id

    waiting_order_id = context.user_data.get("waiting_sponsor_order_id")

    if waiting_order_id:
        channel_username = normalize_channel_username(text)

        try:
            member = await context.bot.get_chat_member(channel_username, context.bot.id)
            if member.status not in ["administrator", "creator"]:
                await update.message.reply_text("❌ Бот должен быть администратором указанного канала.")
                return
        except Exception:
            await update.message.reply_text(
                "❌ Не удалось проверить канал. Убедитесь, что username верный и бот добавлен в админы."
            )
            return

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE sponsor_orders
                    SET channel_username = %s,
                        status = 'approved'
                    WHERE id = %s AND user_id = %s
                    """,
                    (channel_username, waiting_order_id, uid),
                )
                conn.commit()

        context.user_data.pop("waiting_sponsor_order_id", None)
        await place_next_temp_order(context)

        await update.message.reply_text(
            f"✅ Канал {channel_username} сохранён.\nЗаявка одобрена и будет поставлена в очередь."
        )
        return

    if text == "👤 Профиль":
        await show_profile(update, uid, user.first_name or "друг", context)
        return

    if text == "❓ Помощь":
        context.user_data["faq_open_key"] = None
        await update.message.reply_text(
            build_faq_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=get_faq_keyboard(),
        )
        return

    if text == "🔄 Обмен звёзд":
        state = await get_user_state(uid, context)
        await update.message.reply_text(
            (
                f"🔄 <b>Обмен звёзд</b>\n\n"
                f"⭐ Ваш баланс: <b>{state['stars']}</b>\n\n"
                f"Выберите нужное действие:"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=get_exchange_inline(),
        )
        return

    if text == "🏆 Лидерборд":
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT first_name, username, COALESCE(tickets, 0) AS stars
                        FROM users
                        ORDER BY stars DESC, user_id ASC
                        LIMIT 10
                        """
                    )
                    rows = cur.fetchall()

            if not rows:
                await update.message.reply_text("Лидерборд пока пуст.")
                return

            msg = "🏆 <b>Лидерборд</b>\n"
            for i, (first_name, username, stars) in enumerate(rows, 1):
                name = first_name or display_username(username)
                msg += f"{i}. <b>{name}</b> — {stars}⭐\n"

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
            ])
            await update.message.reply_text(
                msg,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        except Exception as e:
            await update.message.reply_text(f"Ошибка: {e}")
        return

    if text.startswith("🌠 Звёздное Колесо"):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ])
        await update.message.reply_text(
            "Откройте WebApp кнопкой в меню ниже 👇",
            reply_markup=keyboard,
        )
        return

    await update.message.reply_text("Используйте кнопки меню.")


async def sponsor_slots_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        s.slot_no,
                        s.sponsor_type,
                        s.channel_username,
                        s.order_id,
                        s.is_active,
                        o.target_subscribers,
                        o.counted_subscribers,
                        o.active_subscribers
                    FROM sponsor_slots s
                    LEFT JOIN sponsor_orders o ON o.id = s.order_id
                    ORDER BY s.slot_no
                    """
                )
                rows = cur.fetchall()

        text = "📢 <b>Слоты спонсоров</b>\n\n"
        for row in rows:
            slot_no, sponsor_type, channel_username, order_id, is_active, target, counted, active = row

            if not is_active or not channel_username:
                text += f"Слот {slot_no}: пусто\n\n"
                continue

            if sponsor_type == "main":
                text += f"Слот {slot_no} (основной): {channel_username}\n\n"
            else:
                text += (
                    f"Слот {slot_no} (временный): {channel_username}\n"
                    f"Заказ #{order_id}\n"
                    f"Привлечено: {counted or 0}/{target or 0}\n"
                    f"Удержано: {active or 0}\n\n"
                )

        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def sponsor_queue_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, username, channel_username, priority_level, stars_amount, status, created_at
                    FROM sponsor_orders
                    WHERE status IN ('waiting_link', 'approved', 'active')
                    ORDER BY
                        CASE
                            WHEN status = 'active' THEN 0
                            WHEN status = 'approved' THEN 1
                            ELSE 2
                        END,
                        priority_level DESC,
                        created_at ASC
                    """
                )
                rows = cur.fetchall()

        if not rows:
            await update.message.reply_text("Очередь пуста.")
            return

        text = "🗂 <b>Очередь / активные заказы</b>\n\n"
        for row in rows:
            order_id, username, channel_username, priority_level, stars_amount, status, created_at = row
            text += (
                f"#{order_id} | {display_username(username)}\n"
                f"Канал: {channel_username or 'не указан'}\n"
                f"Статус: {status}\n"
                f"Приоритет: {'высокий' if priority_level == 1 else 'обычный'}\n"
                f"Оплачено: {stars_amount}⭐\n\n"
            )

        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def set_main_sponsor_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    if len(context.args) < 2:
        await update.message.reply_text("Использование: /set_main_sponsor <1|2> <@channel>")
        return

    try:
        slot_no = int(context.args[0])
        channel_username = normalize_channel_username(context.args[1])

        if slot_no not in (1, 2):
            await update.message.reply_text("Можно указать только слот 1 или 2.")
            return

        member = await context.bot.get_chat_member(channel_username, context.bot.id)
        if member.status not in ["administrator", "creator"]:
            await update.message.reply_text("Бот не является администратором этого канала.")
            return

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE sponsor_slots
                    SET channel_username = %s,
                        is_active = TRUE,
                        order_id = NULL
                    WHERE slot_no = %s
                    """,
                    (channel_username, slot_no),
                )
                conn.commit()

        await update.message.reply_text(
            f"✅ Основной спонсор для слота {slot_no} установлен: {channel_username}"
        )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def remove_temp_sponsor_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT order_id FROM sponsor_slots WHERE slot_no = 3")
                row = cur.fetchone()
                order_id = row[0] if row else None

                if order_id:
                    cur.execute(
                        """
                        UPDATE sponsor_orders
                        SET status = 'completed',
                            completed_at = %s
                        WHERE id = %s
                        """,
                        (utcnow(), order_id),
                    )

                cur.execute(
                    """
                    UPDATE sponsor_slots
                    SET channel_username = NULL,
                        order_id = NULL,
                        is_active = FALSE
                    WHERE slot_no = 3
                    """
                )
                conn.commit()

        await update.message.reply_text("✅ Временный спонсор удалён из слота 3.")
        await place_next_temp_order(context)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def check_sponsor_progress_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    await recount_temp_order_progress(context)
    await update.message.reply_text("✅ Прогресс временного спонсора обновлён.")


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    if not context.args:
        await update.message.reply_text("Введите текст.")
        return

    msg = " ".join(context.args)
    await update.message.reply_text("⏳ Рассылка...")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM users")
                users = cur.fetchall()

        count = 0
        for (uid,) in users:
            try:
                await context.bot.send_message(uid, msg)
                count += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        await update.message.reply_text(f"✅ Доставлено: {count}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                total_users = cur.fetchone()[0]

                cur.execute("SELECT COALESCE(SUM(tickets), 0) FROM users")
                total_stars = cur.fetchone()[0] or 0

                cur.execute("SELECT COUNT(*) FROM users WHERE COALESCE(activated, FALSE) = TRUE")
                activated_users = cur.fetchone()[0]

        text = (
            f"📊 <b>СТАТИСТИКА БОТА:</b>\n\n"
            f"👥 <b>Всего пользователей:</b> {total_users}\n"
            f"🚀 <b>Активировали колесо:</b> {activated_users}\n"
            f"⭐ <b>Всего звёзд в системе:</b> {total_stars}\n"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка получения статистики: {e}")


async def weekly_bonus_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    success_count = 0
    skipped_count = 0

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM users")
                users = [row[0] for row in cur.fetchall()]

        for user_id in users:
            ok, _ = await process_weekly_hold_bonus(user_id, context)
            if ok:
                success_count += 1
            else:
                skipped_count += 1

        await update.message.reply_text(
            f"✅ Недельный бонус обработан.\n"
            f"Начислено: {success_count}\n"
            f"Пропущено: {skipped_count}"
        )
    except Exception as e:
        await update.message.reply_text(f"Ошибка начисления бонусов: {e}")


async def stop_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    global IS_ACTIVE
    IS_ACTIVE = False
    await update.message.reply_text("⛔️ <b>ПАУЗА</b>", parse_mode=ParseMode.HTML)


async def resume_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    global IS_ACTIVE
    IS_ACTIVE = True
    await update.message.reply_text("▶️ <b>БОТ АКТИВЕН</b>", parse_mode=ParseMode.HTML)



def make_progress_bar(current, target, length=10):
    if target <= 0:
        return "🟩" * length
    filled = max(0, min(int((current / target) * length), length))
    empty = length - filled
    return "🟩" * filled + "⬜" * empty


def get_level_progress_data(ref_count: int):
    level = get_level_info(ref_count)

    if level["name"] == "Bronze":
        base = 0
        target = 5
        current_in_level = ref_count
    elif level["name"] == "Silver":
        base = 5
        target = 10
        current_in_level = ref_count - base
    elif level["name"] == "Gold":
        base = 10
        target = 15
        current_in_level = ref_count - base
    else:
        return {
            "level": level,
            "progress_label": "📈 <b>Максимальный уровень достигнут</b>",
            "progress_bar": "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩",
            "remaining_text": "",
        }

    step_target = target - base
    progress_bar = make_progress_bar(current_in_level, step_target, length=10)
    left = max(0, target - ref_count)

    return {
        "level": level,
        "progress_label": (
            f"📈 <b>До уровня {level['next_name']}</b>: "
            f"<b>{ref_count}/{target}</b>"
        ),
        "progress_bar": progress_bar,
        "remaining_text": f"(осталось {left})",
    }


def get_faq_keyboard(open_key=None):
    items = [
        ("start", "🔰 Начало работы"),
        ("wheel", "🌠 Звёздное Колесо"),
        ("levels", "🏅 Уровни и бонусы"),
        ("weekly", "🎁 Еженедельный бонус"),
        ("exchange", "💱 Обмен звёзд"),
        ("sponsors", "📢 Спонсорская программа"),
        ("profile", "👤 Профиль"),
        ("problems", "❓ Частые проблемы"),
    ]

    keyboard = []
    for key, title in items:
        prefix = "▼ " if key == open_key else "▶ "
        keyboard.append([InlineKeyboardButton(f"{prefix}{title}", callback_data=f"faq:{key}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(keyboard)


def build_faq_text(selected_key=None):
    ordered_keys = [
        "start",
        "wheel",
        "levels",
        "weekly",
        "exchange",
        "sponsors",
        "profile",
        "problems",
    ]

    text = "❓ <b>Помощь</b>\n\nНажмите на интересующий раздел:\n"

    for key in ordered_keys:
        item = FAQ_ITEMS[key]
        if key == selected_key:
            text += f"\n▼ <b>{item['title']}</b>\n\n{item['text']}\n"
        else:
            text += f"\n▶ {item['title']}"

    return text


def get_reply_menu(user_id: int, bonus_percent: int = 0):
    return ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(
                    f"🌠 Звёздное Колесо (+{bonus_percent}% к выигрышу)",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL}?user_id={user_id}"),
                ),
            ],
            [
                KeyboardButton("👤 Профиль"),
                KeyboardButton("🔄 Обмен звёзд"),
            ],
            [
                KeyboardButton("🏆 Лидерборд"),
                KeyboardButton("❓ Помощь"),
            ],
        ],
        resize_keyboard=True,
    )


async def get_start_text(user_id, first_name, context):
    state = await get_user_state(user_id, context)
    progress = get_level_progress_data(state["ref_count"])
    reflink = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"

    activation_text = (
        "✅ <b>Доступ к колесу активирован</b>"
        if state["activated"]
        else "⚠️ <b>Для открытия колеса пригласите 2 активных реферала</b>"
    )

    if not state["all_subs_ok"]:
        wheel_access = "❌ <b>Звёздное Колесо недоступно: подпишитесь на всех активных спонсоров</b>"
    elif not state["activated"]:
        wheel_access = "❌ <b>Звёздное Колесо недоступно: не хватает 2 активных рефералов</b>"
    else:
        wheel_access = "✅ <b>Звёздное Колесо доступно</b>"

    return (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        f"Крути <b>Звёздное Колесо</b> — выигрывай звёзды ⭐\n"
        f"Звёзды меняй на призы: Premium, вывод, продвижение канала!\n\n"
        f"🚀 <b>Как начать за 3 шага:</b>\n"
        f"1️⃣ Подпишись на каналы спонсоров ✅\n"
        f"2️⃣ Пригласи 2 друзей по своей ссылке\n"
        f"3️⃣ Крути колесо и забирай звёзды!\n\n"
        f"💡 <b>Бесплатное вращение</b> — раз в 6 часов.\n"
        f"Хочешь чаще? Трать звёзды на спины — {EXTRA_SPIN_COST}⭐ за спин\n"
        f"Начисление каждую неделю по {WEEKLY_HOLD_BONUS}⭐ "
        f"(если не отписались от спонсоров)\n\n"
        f"📌 <b>Активные спонсоры:</b>\n{state['channels_list']}\n"
        f"👥 <b>Активные рефералы:</b> {state['ref_count']}\n"
        f"🔗 <b>Ваша реферальная ссылка:</b>\n"
        f"<code>{reflink}</code>\n\n"
        f"🏅 <b>Уровень:</b> {state['level']['emoji']} {state['level']['name']}\n"
        f"{progress['progress_label']}\n"
        f"{progress['progress_bar']} {progress['remaining_text']}\n"
        f"✨ <b>Бонус:</b> +{state['level']['bonus_percent']}% к выигрышу\n\n"
        f"{activation_text}\n"
        f"{wheel_access}"
    )


async def show_profile(query_or_update, user_id: int, first_name: str, context: ContextTypes.DEFAULT_TYPE, edit=False):
    decay_result = await apply_inactivity_decay(user_id, context)
    state = await get_user_state(user_id, context)
    me = query_or_update.from_user if hasattr(query_or_update, "from_user") else query_or_update.effective_user
    reflink = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"
    progress = get_level_progress_data(state["ref_count"])

    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"Имя: <b>{first_name}</b>\n"
        f"Username: <b>{display_username(me.username)}</b>\n"
        f"ID: <code>{user_id}</code>\n\n"
        f"⭐ <b>Баланс:</b> {state['stars']}\n"
        f"🎁 <b>Недельных бонусов получено:</b> "
        f"{state['weekly_hold_bonus_count']}/{MAX_WEEKLY_HOLD_BONUSES}\n\n"
        f"👥 <b>Активные рефералы:</b> {state['ref_count']}\n"
        f"🔗 <b>Ваша реферальная ссылка:</b>\n"
        f"<code>{reflink}</code>\n\n"
        f"🏅 <b>Уровень:</b> {state['level']['emoji']} {state['level']['name']}\n"
        f"{progress['progress_label']}\n"
        f"{progress['progress_bar']} {progress['remaining_text']}\n"
        f"✨ <b>Бонус:</b> +{state['level']['bonus_percent']}% к выигрышу"
    )

    if decay_result.get("decayed", 0) > 0:
        text += (
            f"\n\n⚠️ <b>За период неактивности было списано:</b> {decay_result['decayed']}⭐\n"
            f"Чтобы сохранять баланс, заходите в бот хотя бы 1 раз в 7 дней."
        )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])

    if edit:
        await query_or_update.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        await query_or_update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def faq_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    selected_key = None
    if query.data.startswith("faq:"):
        clicked_key = query.data.split(":", 1)[1]
        current_open_key = context.user_data.get("faq_open_key")

        if current_open_key == clicked_key:
            context.user_data["faq_open_key"] = None
            selected_key = None
        else:
            context.user_data["faq_open_key"] = clicked_key
            selected_key = clicked_key

    try:
        await query.edit_message_text(
            build_faq_text(selected_key),
            parse_mode=ParseMode.HTML,
            reply_markup=get_faq_keyboard(selected_key),
        )
    except Exception as e:
        if "not modified" in str(e).lower():
            pass
        else:
            raise e



def make_progress_bar(current, target, length=10):
    if target <= 0:
        return "🟩" * length
    filled = max(0, min(int((current / target) * length), length))
    empty = length - filled
    return "🟩" * filled + "⬜" * empty


def get_level_progress_data(ref_count: int):
    level = get_level_info(ref_count)

    if level["name"] == "Bronze":
        base = 0
        target = 5
        current_in_level = ref_count
    elif level["name"] == "Silver":
        base = 5
        target = 10
        current_in_level = ref_count - base
    elif level["name"] == "Gold":
        base = 10
        target = 15
        current_in_level = ref_count - base
    else:
        return {
            "level": level,
            "progress_label": "📈 <b>Максимальный уровень достигнут</b>",
            "progress_bar": "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩",
            "remaining_text": "",
        }

    step_target = target - base
    progress_bar = make_progress_bar(current_in_level, step_target, length=10)
    left = max(0, target - ref_count)

    return {
        "level": level,
        "progress_label": (
            f"📈 <b>До уровня {level['next_name']}</b>: "
            f"<b>{ref_count}/{target}</b>"
        ),
        "progress_bar": progress_bar,
        "remaining_text": f"(осталось {left})",
    }


def get_faq_keyboard(open_key=None):
    items = [
        ("start", "🔰 Начало работы"),
        ("wheel", "🌠 Звёздное Колесо"),
        ("levels", "🏅 Уровни и бонусы"),
        ("weekly", "🎁 Еженедельный бонус"),
        ("exchange", "💱 Обмен звёзд"),
        ("sponsors", "📢 Спонсорская программа"),
        ("profile", "👤 Профиль"),
        ("problems", "❓ Частые проблемы"),
    ]

    keyboard = []
    for key, title in items:
        prefix = "▼ " if key == open_key else "▶ "
        keyboard.append([InlineKeyboardButton(f"{prefix}{title}", callback_data=f"faq:{key}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(keyboard)


def build_faq_text(selected_key=None):
    ordered_keys = [
        "start",
        "wheel",
        "levels",
        "weekly",
        "exchange",
        "sponsors",
        "profile",
        "problems",
    ]

    text = "❓ <b>Помощь</b>\n\nНажмите на интересующий раздел:\n"

    for key in ordered_keys:
        item = FAQ_ITEMS[key]
        if key == selected_key:
            text += f"\n▼ <b>{item['title']}</b>\n\n{item['text']}\n"
        else:
            text += f"\n▶ {item['title']}"

    return text


def get_reply_menu(user_id: int, bonus_percent: int = 0):
    return ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(
                    f"🌠 Звёздное Колесо (+{bonus_percent}% к выигрышу)",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL}?user_id={user_id}"),
                ),
            ],
            [
                KeyboardButton("👤 Профиль"),
                KeyboardButton("🔄 Обмен звёзд"),
            ],
            [
                KeyboardButton("🏆 Лидерборд"),
                KeyboardButton("❓ Помощь"),
            ],
        ],
        resize_keyboard=True,
    )


async def get_start_text(user_id, first_name, context):
    state = await get_user_state(user_id, context)
    progress = get_level_progress_data(state["ref_count"])
    reflink = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"

    activation_text = (
        "✅ <b>Доступ к колесу активирован</b>"
        if state["activated"]
        else "⚠️ <b>Для открытия колеса пригласите 2 активных реферала</b>"
    )

    if not state["all_subs_ok"]:
        wheel_access = "❌ <b>Звёздное Колесо недоступно: подпишитесь на всех активных спонсоров</b>"
    elif not state["activated"]:
        wheel_access = "❌ <b>Звёздное Колесо недоступно: не хватает 2 активных рефералов</b>"
    else:
        wheel_access = "✅ <b>Звёздное Колесо доступно</b>"

    return (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        f"Крути <b>Звёздное Колесо</b> — выигрывай звёзды ⭐\n"
        f"Звёзды меняй на призы: Premium, вывод, продвижение канала!\n\n"
        f"🚀 <b>Как начать за 3 шага:</b>\n"
        f"1️⃣ Подпишись на каналы спонсоров ✅\n"
        f"2️⃣ Пригласи 2 друзей по своей ссылке\n"
        f"3️⃣ Крути колесо и забирай звёзды!\n\n"
        f"💡 <b>Бесплатное вращение</b> — раз в 6 часов.\n"
        f"Хочешь чаще? Трать звёзды на спины — {EXTRA_SPIN_COST}⭐ за спин\n"
        f"Начисление каждую неделю по {WEEKLY_HOLD_BONUS}⭐ "
        f"(если не отписались от спонсоров)\n\n"
        f"📌 <b>Активные спонсоры:</b>\n{state['channels_list']}\n"
        f"👥 <b>Активные рефералы:</b> {state['ref_count']}\n"
        f"🔗 <b>Ваша реферальная ссылка:</b>\n"
        f"<code>{reflink}</code>\n\n"
        f"🏅 <b>Уровень:</b> {state['level']['emoji']} {state['level']['name']}\n"
        f"{progress['progress_label']}\n"
        f"{progress['progress_bar']} {progress['remaining_text']}\n"
        f"✨ <b>Бонус:</b> +{state['level']['bonus_percent']}% к выигрышу\n\n"
        f"{activation_text}\n"
        f"{wheel_access}"
    )


async def show_profile(query_or_update, user_id: int, first_name: str, context: ContextTypes.DEFAULT_TYPE, edit=False):
    decay_result = await apply_inactivity_decay(user_id, context)
    state = await get_user_state(user_id, context)
    me = query_or_update.from_user if hasattr(query_or_update, "from_user") else query_or_update.effective_user
    reflink = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"
    progress = get_level_progress_data(state["ref_count"])

    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"Имя: <b>{first_name}</b>\n"
        f"Username: <b>{display_username(me.username)}</b>\n"
        f"ID: <code>{user_id}</code>\n\n"
        f"⭐ <b>Баланс:</b> {state['stars']}\n"
        f"🎁 <b>Недельных бонусов получено:</b> "
        f"{state['weekly_hold_bonus_count']}/{MAX_WEEKLY_HOLD_BONUSES}\n\n"
        f"👥 <b>Активные рефералы:</b> {state['ref_count']}\n"
        f"🔗 <b>Ваша реферальная ссылка:</b>\n"
        f"<code>{reflink}</code>\n\n"
        f"🏅 <b>Уровень:</b> {state['level']['emoji']} {state['level']['name']}\n"
        f"{progress['progress_label']}\n"
        f"{progress['progress_bar']} {progress['remaining_text']}\n"
        f"✨ <b>Бонус:</b> +{state['level']['bonus_percent']}% к выигрышу"
    )

    if decay_result.get("decayed", 0) > 0:
        text += (
            f"\n\n⚠️ <b>За период неактивности было списано:</b> {decay_result['decayed']}⭐\n"
            f"Чтобы сохранять баланс, заходите в бот хотя бы 1 раз в 7 дней."
        )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])

    if edit:
        await query_or_update.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        await query_or_update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def faq_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    selected_key = None
    if query.data.startswith("faq:"):
        clicked_key = query.data.split(":", 1)[1]
        current_open_key = context.user_data.get("faq_open_key")

        if current_open_key == clicked_key:
            context.user_data["faq_open_key"] = None
            selected_key = None
        else:
            context.user_data["faq_open_key"] = clicked_key
            selected_key = clicked_key

    try:
        await query.edit_message_text(
            build_faq_text(selected_key),
            parse_mode=ParseMode.HTML,
            reply_markup=get_faq_keyboard(selected_key),
        )
    except Exception as e:
        if "not modified" in str(e).lower():
            pass
        else:
            raise e


def main():
    init_db()

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("weekly_bonus", weekly_bonus_all))
    app.add_handler(CommandHandler("stop", stop_bot))
    app.add_handler(CommandHandler("resume", resume_bot))

    app.add_handler(CommandHandler("sponsor_slots", sponsor_slots_cmd))
    app.add_handler(CommandHandler("sponsor_queue", sponsor_queue_cmd))
    app.add_handler(CommandHandler("set_main_sponsor", set_main_sponsor_cmd))
    app.add_handler(CommandHandler("remove_temp_sponsor", remove_temp_sponsor_cmd))
    app.add_handler(CommandHandler("check_sponsor_progress", check_sponsor_progress_cmd))

    app.add_handler(CallbackQueryHandler(faq_callback, pattern=r"^faq:"))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_menu_handler))

    print("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()
