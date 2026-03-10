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
MY_DATABASE_URL = os.getenv("MY_DATABASE_URL")

SPONSORS = ["@openbusines", "@MTProxy_russia", "@SAGkatalog"]
ADMINS = [514167463]
BOT_USERNAME_FOR_REFLINK = "moy_giveaway_bot"
WEBAPP_URL = "https://moygivawaybot.ru/index.html"

IS_ACTIVE = True

START_BONUS = 5
WEEKLY_HOLD_BONUS = 10
MAX_WEEKLY_HOLD_BONUSES = 4
EXTRA_SPIN_COST = 1

PREMIUM_COST = 700
CHANNEL_PROMO_COST = 100
PROFILE_BADGE_COST = 20

FAQ_CB = "faq"


def get_db_connection():
    if not MY_DATABASE_URL:
        raise RuntimeError("MY_DATABASE_URL is not set")
    return psycopg2.connect(MY_DATABASE_URL)


def to_naive_utc(dt):
    if dt is None:
        return None
    return dt.replace(tzinfo=None)


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def display_username(username: str) -> str:
    if not username:
        return "Без ника"
    return f"@{username.lstrip('@')}"


async def check_subscription(user_id, channel, context):
    try:
        member = await context.bot.get_chat_member(chat_id=channel, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False


def get_level_info(ref_count: int):
    if ref_count >= 15:
        return {
            "name": "VIP",
            "emoji": "🌟",
            "bonus_percent": 60,
            "multiplier": 1.60,
            "next_target": None,
            "next_name": None,
            "expected_value": 1.70,
        }
    if ref_count >= 10:
        return {
            "name": "Gold",
            "emoji": "🥇",
            "bonus_percent": 35,
            "multiplier": 1.35,
            "next_target": 15,
            "next_name": "VIP",
            "expected_value": 1.43,
        }
    if ref_count >= 5:
        return {
            "name": "Silver",
            "emoji": "🥈",
            "bonus_percent": 15,
            "multiplier": 1.15,
            "next_target": 10,
            "next_name": "Gold",
            "expected_value": 1.22,
        }
    return {
        "name": "Bronze",
        "emoji": "🥉",
        "bonus_percent": 0,
        "multiplier": 1.00,
        "next_target": 5,
        "next_name": "Silver",
        "expected_value": 1.06,
    }


def get_reply_menu(user_id: int):
    return ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(
                    "🌠 Звёздное Колесо",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL}?user_id={user_id}"),
                )
            ],
            [KeyboardButton("👤 Профиль"), KeyboardButton("🔄 Обмен звёзд")],
            [KeyboardButton("🔗 Моя ссылка"), KeyboardButton("📚 FAQ")],
        ],
        resize_keyboard=True,
    )


def get_main_inline():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="check_sub")],
            [InlineKeyboardButton("👤 Профиль", callback_data="profile")],
            [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
            [InlineKeyboardButton("🔄 Обмен звёзд", callback_data="exchange")],
            [InlineKeyboardButton("📚 FAQ", callback_data=FAQ_CB)],
            [InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard")],
        ]
    )


def get_exchange_inline():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"💎 Telegram Premium — {PREMIUM_COST} ⭐", callback_data="exchange_premium")],
            [InlineKeyboardButton("💸 Вывод звёзд", callback_data="exchange_withdraw")],
            [InlineKeyboardButton(f"📢 Промо канала — {CHANNEL_PROMO_COST} ⭐", callback_data="exchange_promo")],
            [InlineKeyboardButton(f"🏅 Украшение профиля — {PROFILE_BADGE_COST} ⭐", callback_data="exchange_badge")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")],
        ]
    )


FAQ_TEXT = f"""
📚 <b>FAQ — Звёздное Колесо</b>

🌠 <b>Как получить доступ к Звёздному Колесу?</b>
1. Подпишитесь на все актуальные каналы спонсоров
2. Пригласите 2 активных реферала

⚠️ Даже после активации вы должны оставаться подписанными на все текущие каналы спонсоров.

⭐ <b>Как получать звёзды?</b>
• Вращать Звёздное Колесо — бесплатно 1 раз в 6 часов
• Купить дополнительный спин за {EXTRA_SPIN_COST}⭐
• Удерживать подписку на всех спонсоров — 1 раз в неделю начисляется бонус
• Приглашать друзей и повышать уровень

🏅 <b>Уровни и бонусы:</b>
🥉 Bronze — 2 активных реферала, базовые шансы
🥈 Silver — 5 активных рефералов, +15% к выигрышным секторам
🥇 Gold — 10 активных рефералов, +35% к выигрышным секторам
🌟 VIP — 15 активных рефералов, +60% к выигрышным секторам

🎡 <b>Как работает бонус уровня?</b>
Бонус применяется ко всем выигрышным секторам колеса:
1⭐, 2⭐, 3⭐, 4⭐, 5⭐
Сектор «ничего» уменьшается так, чтобы сумма вероятностей была 100%.

🔄 <b>Обмен звёзд:</b>
• Telegram Premium
• Вывод звёзд
• Промо канала
• Украшение профиля

❓ Поддержка: @moderatorgive_bot
"""


def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
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

                alter_statements = [
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS activated BOOLEAN DEFAULT FALSE",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS all_subscribed INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS tickets INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_fortune_time TIMESTAMP NULL",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS lifetime_ref_count INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS weekly_hold_bonus_count INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_hold_bonus_at TIMESTAMP NULL",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_badge BOOLEAN DEFAULT FALSE",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_level_notified TEXT DEFAULT 'Bronze'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP NULL",
                ]

                for stmt in alter_statements:
                    try:
                        cursor.execute(stmt)
                    except Exception as e:
                        print("ALTER warning:", e)

                conn.commit()

        print("✅ База данных подключена и инициализирована.")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")


async def count_valid_refs(referrer_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    valid_count = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT referred_id, COALESCE(is_valid, FALSE) FROM referrals WHERE referrer_id=%s",
                (referrer_id,),
            )
            rows = cur.fetchall()

            for referred_id, is_valid in rows:
                if is_valid:
                    valid_count += 1
                    continue

                subscribed = False
                for channel in SPONSORS:
                    if await check_subscription(referred_id, channel, context):
                        subscribed = True
                        break

                if subscribed:
                    cur.execute(
                        """
                        UPDATE referrals
                        SET is_valid=TRUE, checked_at=%s
                        WHERE referrer_id=%s AND referred_id=%s
                        """,
                        (utcnow(), referrer_id, referred_id),
                    )
                    valid_count += 1

            cur.execute(
                "UPDATE users SET lifetime_ref_count=%s WHERE user_id=%s",
                (valid_count, referrer_id),
            )

            if valid_count >= 2:
                cur.execute(
                    "UPDATE users SET activated=TRUE WHERE user_id=%s",
                    (referrer_id,),
                )

            conn.commit()

    return valid_count


async def get_user_state(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    all_subs_ok = True
    channels_list = ""

    for i, ch in enumerate(SPONSORS, 1):
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

        channels_list += f"{i}. {ch} {icon}\n"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET all_subscribed=%s WHERE user_id=%s",
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
                    COALESCE(profile_badge, FALSE),
                    COALESCE(last_level_notified, 'Bronze')
                FROM users
                WHERE user_id=%s
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
    profile_badge = False
    last_level_notified = "Bronze"

    if row:
        (
            activated,
            ref_count,
            stars,
            weekly_hold_bonus_count,
            last_fortune_time,
            profile_badge,
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
        "profile_badge": profile_badge,
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
                    "SELECT COALESCE(last_level_notified, 'Bronze') FROM users WHERE user_id=%s",
                    (user_id,),
                )
                row = cur.fetchone()
                prev_level = row[0] if row else "Bronze"

                if prev_level != current_level:
                    cur.execute(
                        "UPDATE users SET last_level_notified=%s WHERE user_id=%s",
                        (current_level, user_id),
                    )
                    conn.commit()

                    await context.bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"🎉 <b>Поздравляем!</b>\n\n"
                            f"Ваш уровень повышен до <b>{state['level']['emoji']} {current_level}</b>\n"
                            f"Бонус к выигрышным секторам: <b>+{state['level']['bonus_percent']}%</b>\n"
                            f"Средний доход со спина: <b>~{state['level']['expected_value']:.2f} ⭐</b>"
                        ),
                        parse_mode=ParseMode.HTML,
                    )
    except Exception as e:
        print("notify_level_up_if_needed error:", e)


async def get_start_text(user_id, first_name, context):
    state = await get_user_state(user_id, context)

    activation_text = (
        "✅ <b>Доступ к колесу активирован</b>\n"
        if state["activated"]
        else "⚠️ <b>Для открытия колеса пригласите 2 активных реферала</b>\n"
    )

    if not state["all_subs_ok"]:
        wheel_access = "❌ <b>Звёздное Колесо недоступно: подпишитесь на всех спонсоров</b>"
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

    cooldown_text = "✅ Можно крутить прямо сейчас"
    if state["last_fortune_time"]:
        delta = utcnow() - state["last_fortune_time"]
        if delta < timedelta(hours=6):
            seconds_left = int(timedelta(hours=6).total_seconds() - delta.total_seconds())
            h_left = seconds_left // 3600
            m_left = (seconds_left % 3600) // 60
            cooldown_text = f"⏳ До следующей бесплатной крутки: {h_left}ч {m_left}м"

    return (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        f"🌠 <b>Добро пожаловать в Звёздное Колесо</b>\n\n"
        f"{activation_text}"
        f"{wheel_access}\n\n"
        f"⭐ <b>Ваш баланс:</b> {state['stars']}\n"
        f"🏅 <b>Ваш уровень:</b> {state['level']['emoji']} {state['level']['name']}\n"
        f"🎯 <b>Бонус уровня:</b> +{state['level']['bonus_percent']}% к выигрышным секторам\n"
        f"📊 <b>Средний доход со спина:</b> ~{state['level']['expected_value']:.2f} ⭐\n"
        f"{progress_text}\n"
        f"🔄 <b>Статус колеса:</b> {cooldown_text}\n"
        f"💫 <b>Доп. вращение:</b> доступно за {EXTRA_SPIN_COST}⭐\n\n"
        f"📌 <b>Спонсоры:</b>\n{state['channels_list']}\n"
        f"👥 <b>Активные рефералы:</b> {state['ref_count']}\n"
        f"🎁 <b>Недельных бонусов получено:</b> "
        f"{state['weekly_hold_bonus_count']}/{MAX_WEEKLY_HOLD_BONUSES}\n"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not IS_ACTIVE:
        await update.message.reply_text("⛔️ Бот временно на паузе.", parse_mode=ParseMode.HTML)
        return

    user = update.effective_user
    uid = user.id
    first_name = user.first_name
    username = user.username
    is_new_user = False

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM users WHERE user_id=%s", (uid,))
                exists = cur.fetchone()

                if not exists:
                    is_new_user = True
                    cur.execute(
                        """
                        INSERT INTO users (
                            user_id, username, tickets, activated, all_subscribed,
                            lifetime_ref_count, weekly_hold_bonus_count, profile_badge,
                            last_level_notified, last_seen
                        )
                        VALUES (%s, %s, %s, FALSE, 0, 0, 0, FALSE, 'Bronze', %s)
                        """,
                        (uid, username, START_BONUS, utcnow()),
                    )
                else:
                    cur.execute(
                        "UPDATE users SET username=%s, last_seen=%s WHERE user_id=%s",
                        (username, utcnow(), uid),
                    )

                conn.commit()
    except Exception as e:
        print("start user save error:", e)

    if context.args:
        ref_str = context.args[0]
        if ref_str.isdigit() and int(ref_str) != uid:
            referrer = int(ref_str)
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO referrals (referrer_id, referred_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (referrer, uid),
                        )
                        conn.commit()
            except Exception as e:
                print("ref insert error:", e)

    await count_valid_refs(uid, context)

    if context.args:
        ref_str = context.args[0]
        if ref_str.isdigit() and int(ref_str) != uid:
            referrer = int(ref_str)
            await count_valid_refs(referrer, context)
            await notify_level_up_if_needed(referrer, context)

    hello_text = "🎁 Вам начислено <b>5 стартовых звёзд!</b>\n\n" if is_new_user else ""

    await update.message.reply_text(
        hello_text + "Откройте меню ниже и начните путь к наградам.",
        parse_mode=ParseMode.HTML,
        reply_markup=get_reply_menu(uid),
    )

    text = await get_start_text(uid, first_name, context)
    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_inline(),
    )


async def show_profile(chat_or_query, user_id: int, first_name: str, context: ContextTypes.DEFAULT_TYPE, edit=False):
    state = await get_user_state(user_id, context)

    if state["level"]["next_target"]:
        remain = max(0, state["level"]["next_target"] - state["ref_count"])
        progress = (
            f"📈 До следующего уровня: <b>{state['ref_count']}/{state['level']['next_target']}</b>\n"
            f"Осталось пригласить: <b>{remain}</b>"
        )
    else:
        progress = "👑 Достигнут максимальный уровень"

    wheel_status = "Доступно ✅" if state["activated"] and state["all_subs_ok"] else "Недоступно ❌"

    text = (
        f"👤 <b>Профиль {first_name}</b>\n\n"
        f"⭐ <b>Баланс:</b> {state['stars']}\n"
        f"🏅 <b>Уровень:</b> {state['level']['emoji']} {state['level']['name']}\n"
        f"🎯 <b>Бонус уровня:</b> +{state['level']['bonus_percent']}% к выигрышным секторам\n"
        f"📊 <b>Средний доход со спина:</b> ~{state['level']['expected_value']:.2f} ⭐\n"
        f"👥 <b>Активные рефералы:</b> {state['ref_count']}\n"
        f"{progress}\n\n"
        f"🔄 <b>Доступ к колесу:</b> {wheel_status}\n"
        f"💫 <b>Доп. спин:</b> {EXTRA_SPIN_COST}⭐\n"
        f"🎁 <b>Недельных бонусов:</b> {state['weekly_hold_bonus_count']}/{MAX_WEEKLY_HOLD_BONUSES}\n"
        f"🏅 <b>Украшение профиля:</b> {'Есть' if state['profile_badge'] else 'Нет'}"
    )

    if edit:
        await chat_or_query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            ),
        )
    else:
        await chat_or_query.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            ),
        )


async def process_weekly_hold_bonus(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    state = await get_user_state(user_id, context)

    if not state["all_subs_ok"]:
        return False, "Пользователь не подписан на всех спонсоров"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(weekly_hold_bonus_count, 0),
                    last_hold_bonus_at
                FROM users
                WHERE user_id=%s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if not row:
                return False, "Пользователь не найден"

            bonus_count, last_hold_bonus_at = row
            bonus_count = int(bonus_count or 0)
            last_hold_bonus_at = to_naive_utc(last_hold_bonus_at)

            if bonus_count >= MAX_WEEKLY_HOLD_BONUSES:
                return False, "Лимит недельных бонусов исчерпан"

            now = utcnow()
            if last_hold_bonus_at and (now - last_hold_bonus_at) < timedelta(days=7):
                return False, "Ещё не прошла неделя"

            cur.execute(
                """
                UPDATE users
                SET tickets = COALESCE(tickets, 0) + %s,
                    weekly_hold_bonus_count = COALESCE(weekly_hold_bonus_count, 0) + 1,
                    last_hold_bonus_at = %s
                WHERE user_id = %s
                """,
                (WEEKLY_HOLD_BONUS, now, user_id),
            )
            conn.commit()

    return True, f"Начислено {WEEKLY_HOLD_BONUS} ⭐"


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not IS_ACTIVE:
        await query.edit_message_text(
            "⛔️ Бот временно на паузе.",
            parse_mode=ParseMode.HTML,
        )
        return

    uid = query.from_user.id
    data = query.data

    if data in ("check_sub", "back_to_main"):
        await count_valid_refs(uid, context)
        text = await get_start_text(uid, query.from_user.first_name, context)
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_inline(),
        )

    elif data == "profile":
        await show_profile(query, uid, query.from_user.first_name, context, edit=True)

    elif data == "my_reflink":
        link = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={uid}"
        state = await get_user_state(uid, context)

        text = (
            f"🔗 <b>Ваша ссылка для приглашения:</b>\n\n"
            f"<code>{link}</code>\n\n"
            f"👥 Активных рефералов: <b>{state['ref_count']}</b>\n"
            f"Для открытия колеса нужно <b>2</b> активных реферала."
        )
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            ),
        )

    elif data == "exchange":
        state = await get_user_state(uid, context)
        text = (
            f"🔄 <b>Обмен звёзд</b>\n\n"
            f"⭐ Ваш баланс: <b>{state['stars']}</b>\n\n"
            f"Выберите нужное действие:"
        )
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_exchange_inline(),
        )

    elif data == "exchange_premium":
        state = await get_user_state(uid, context)
        if state["level"]["name"] != "VIP":
            await query.edit_message_text(
                "❌ <b>Доступно только для VIP-уровня Звёздного Колеса!</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )
            return

        if state["stars"] < PREMIUM_COST:
            await query.edit_message_text(
                f"❌ Недостаточно звёзд.\nНужно: <b>{PREMIUM_COST} ⭐</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )
            return

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET tickets = tickets - %s WHERE user_id = %s",
                    (PREMIUM_COST, uid),
                )
                cur.execute(
                    """
                    INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (uid, query.from_user.username, "premium", PREMIUM_COST),
                )
                conn.commit()

        await query.edit_message_text(
            "✅ Заявка на Telegram Premium создана. Ожидайте проверки администратора.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
            ),
        )

    elif data == "exchange_withdraw":
        state = await get_user_state(uid, context)
        if state["level"]["name"] != "VIP":
            await query.edit_message_text(
                "❌ <b>Доступно только для VIP-уровня Звёздного Колеса!</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )
            return

        await query.edit_message_text(
            "💸 <b>Вывод звёзд</b>\n\n"
            "Для вывода напишите администратору или добавьте отдельную форму реквизитов.\n"
            "При необходимости можно доработать автоматические заявки на суммы 100/200/500 ⭐.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
            ),
        )

    elif data == "exchange_promo":
        state = await get_user_state(uid, context)
        if state["stars"] < CHANNEL_PROMO_COST:
            await query.edit_message_text(
                f"❌ Недостаточно звёзд.\nНужно: <b>{CHANNEL_PROMO_COST} ⭐</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )
            return

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET tickets = tickets - %s WHERE user_id = %s",
                    (CHANNEL_PROMO_COST, uid),
                )
                cur.execute(
                    """
                    INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (uid, query.from_user.username, "promo", CHANNEL_PROMO_COST),
                )
                conn.commit()

        await query.edit_message_text(
            "✅ Заявка на промо канала создана. Администратор свяжется с вами.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
            ),
        )

    elif data == "exchange_badge":
        state = await get_user_state(uid, context)
        if state["profile_badge"]:
            await query.edit_message_text(
                "ℹ️ Украшение профиля у вас уже активировано.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )
            return

        if state["stars"] < PROFILE_BADGE_COST:
            await query.edit_message_text(
                f"❌ Недостаточно звёзд.\nНужно: <b>{PROFILE_BADGE_COST} ⭐</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )
            return

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET tickets = tickets - %s,
                        profile_badge = TRUE
                    WHERE user_id = %s
                    """,
                    (PROFILE_BADGE_COST, uid),
                )
                conn.commit()

        await query.edit_message_text(
            "✅ Украшение профиля успешно активировано!",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
            ),
        )

    elif data == "leaderboard":
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT username, tickets, COALESCE(lifetime_ref_count, 0)
                        FROM users
                        WHERE tickets > 0
                        ORDER BY tickets DESC
                        LIMIT 10
                        """
                    )
                    rows = cur.fetchall()

            if not rows:
                text = "Пока пусто."
            else:
                text = "🏆 <b>ТОП-10 ПО ЗВЁЗДАМ:</b>\n\n"
                for i, row in enumerate(rows, 1):
                    username, stars, ref_count = row
                    level = get_level_info(int(ref_count or 0))
                    text += f"{i}. {display_username(username)} — {stars} ⭐ {level['emoji']}\n"

        except Exception:
            text = "Ошибка загрузки лидерборда."

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            ),
        )


async def faq_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        FAQ_TEXT,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        ),
    )


async def text_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    uid = update.effective_user.id
    first_name = update.effective_user.first_name

    if text == "👤 Профиль":
        await show_profile(update.message, uid, first_name, context)

    elif text == "🔄 Обмен звёзд":
        state = await get_user_state(uid, context)
        await update.message.reply_text(
            f"🔄 <b>Обмен звёзд</b>\n\n⭐ Ваш баланс: <b>{state['stars']}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=get_exchange_inline(),
        )

    elif text == "🔗 Моя ссылка":
        link = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={uid}"
        await update.message.reply_text(
            f"🔗 <b>Ваша ссылка:</b>\n<code>{link}</code>",
            parse_mode=ParseMode.HTML,
        )

    elif text == "📚 FAQ":
        await update.message.reply_text(
            FAQ_TEXT,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


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

                cur.execute("SELECT COUNT(*) FROM users WHERE COALESCE(all_subscribed,0)=1")
                subscribed_users = cur.fetchone()[0]

                cur.execute("SELECT COALESCE(SUM(tickets),0) FROM users")
                total_stars = cur.fetchone()[0] or 0

                cur.execute("SELECT COUNT(*) FROM users WHERE COALESCE(activated,FALSE)=TRUE")
                activated_users = cur.fetchone()[0]

        text = (
            f"📊 <b>СТАТИСТИКА БОТА:</b>\n\n"
            f"👥 <b>Всего пользователей:</b> {total_users}\n"
            f"✅ <b>Подписаны на спонсоров:</b> {subscribed_users}\n"
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


def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("weekly_bonus", weekly_bonus_all))
    app.add_handler(CommandHandler("stop", stop_bot))
    app.add_handler(CommandHandler("resume", resume_bot))

    app.add_handler(CallbackQueryHandler(faq_callback, pattern="^faq$"))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_menu_handler))

    print("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()
