import os
import json
import asyncio
import random
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

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
MY_DATABASE_URL = os.getenv("MY_DATABASE_URL")

SPONSORS = ["@openbusines", "@MTProxy_russia", "@SAGkatalog"]
PRIZE = "Telegram Premium на 6 месяцев или 1000 ⭐"
ADMINS = [514167463]
BOT_USERNAME_FOR_REFLINK = "moy_giveaway_bot"

IS_ACTIVE = True


# --- Подключение к БД ---
def get_db_connection():
    if not MY_DATABASE_URL:
        # запасной вариант (лучше убрать пароль из кода и держать в .env)
        return psycopg2.connect("postgresql://bot_user:12345@localhost/bot_db")
    return psycopg2.connect(MY_DATABASE_URL)


def to_naive_utc(dt):
    """Привести datetime к UTC-naive (под timestamp without time zone в Postgres)."""
    if dt is None:
        return None
    return dt.replace(tzinfo=None)


def utcnow():
    # ЕДИНЫЙ стандарт времени во всём проекте: UTC-naive
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --- СЕЗОНЫ ---
def get_active_season():
    """Возвращает (season_id, start_at, end_at). Создаёт новый сезон при необходимости."""
    now = utcnow()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, start_at, end_at
                FROM seasons
                WHERE end_at > %s
                ORDER BY end_at ASC
                LIMIT 1
                """,
                (now,),
            )
            row = cur.fetchone()
            if row:
                return row[0], row[1], row[2]

            start_at = now
            end_at = now + timedelta(days=7)
            cur.execute(
                "INSERT INTO seasons (start_at, end_at) VALUES (%s, %s) RETURNING id",
                (start_at, end_at),
            )
            season_id = cur.fetchone()[0]
            conn.commit()
            return season_id, start_at, end_at


def ensure_user_season(user_id: int, season_id: int):
    """
    Если сезон у пользователя сменился/не задан — сбрасываем сезонное
    и привязываем к текущему season_id.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT season_id FROM users WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return

            old_season_id = row[0]
            if old_season_id != season_id:
                cur.execute(
                    """
                    UPDATE users
                    SET
                      season_id = %s,
                      tickets = 0,
                      season_ref_tickets = 0,
                      season_bonus_tickets = 0,
                      last_fortune_time = NULL
                    WHERE user_id = %s
                    """,
                    (season_id, user_id),
                )
                conn.commit()


# --- Вспомогательные функции ---
def mask_username(username: str) -> str:
    if not username:
        return "Без ника"
    username = username.lstrip("@")
    if len(username) <= 3:
        return f"@{username[:1]}***"
    return f"@{username[:2]}***{username[-1]}"


async def check_subscription(user_id, channel, context):
    try:
        member = await context.bot.get_chat_member(chat_id=channel, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        return False


def get_tickets(user_id: int) -> int:
    """В новой модели tickets НЕ пересчитываем. Только читаем."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(tickets, 0) FROM users WHERE user_id=%s", (user_id,))
                row = cur.fetchone()
                return int(row[0] or 0) if row else 0
    except Exception as e:
        print("tickets read error:", e)
        return 0


def get_fortune_shortcut(user_id: int):
    return ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(
                    "🎡 Колесо фортуны",
                    web_app=WebAppInfo(url=f"https://moygivawaybot.ru/index.html?user_id={user_id}"),
                )
            ]
        ],
        resize_keyboard=True,
    )


# --- DB init (без создания users, т.к. уже мигрировали) ---
def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS referrals (
                        referrer_id BIGINT,
                        referred_id BIGINT,
                        UNIQUE(referrer_id, referred_id)
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS winners (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        username TEXT,
                        prize TEXT,
                        win_date TIMESTAMPTZ DEFAULT NOW()
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

                conn.commit()

        print("✅ База данных подключена.")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")


# --- ГЕНЕРАЦИЯ ГЛАВНОГО МЕНЮ ---
async def get_start_text(user_id, first_name, context):
    season_id, season_start, season_end = get_active_season()
    ensure_user_season(user_id, season_id)

    now = utcnow()
    season_end = to_naive_utc(season_end)  # страховка от aware
    left = season_end - now
    if left.total_seconds() < 0:
        left = timedelta(seconds=0)
    days = left.days
    hours = left.seconds // 3600
    minutes = (left.seconds % 3600) // 60

    activated = False
    season_ref_tickets = 0
    season_bonus_tickets = 0
    all_subs_ok = True

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(activated,false),
                           COALESCE(season_ref_tickets,0),
                           COALESCE(season_bonus_tickets,0)
                    FROM users WHERE user_id=%s
                    """,
                    (user_id,),
                )
                row = cur.fetchone()
                if row:
                    activated, season_ref_tickets, season_bonus_tickets = row
    except Exception as e:
        print("read user flags error:", e)

    channels_list = ""
    for i, ch in enumerate(SPONSORS, 1):
        is_sub = await check_subscription(user_id, ch, context)
        if not is_sub:
            all_subs_ok = False
            icon = "❌"
        else:
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
                print(f"Ошибка сохранения подписки на {ch}: {e}")

        channels_list += f"{i}. {ch} {icon}\n"

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET all_subscribed=%s WHERE user_id=%s",
                    (1 if all_subs_ok else 0, user_id),
                )
                conn.commit()
    except Exception as e:
        print("all_subscribed update error:", e)

    tickets = get_tickets(user_id)

    activation_text = (
        "✅ <b>Вы активированы</b> (2 реферала выполнено)\n"
        if activated
        else "⚠️ <b>Не активированы</b>: пригласите <b>2</b> друзей (один раз), чтобы участвовать полноценно.\n"
    )

    msg = (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        f"🎁 <b>Приз недели:</b> {PRIZE}\n\n"
        f"⏳ <b>До конца сезона:</b> {days}д {hours:02d}ч {minutes:02d}м\n\n"
        f"{activation_text}\n"
        f"1️⃣ <b>Подпишись на все каналы спонсоров:</b>\n"
        f"{channels_list}\n"
        f"2️⃣ После активации за каждого нового друга — <b>+1 билет</b> (до <b>10</b> за сезон)\n"
        f"3️⃣ 🎡 Колесо фортуны даёт <b>+1..+5</b> билетов (сверх реф-потолка)\n\n"
        f"🎫 <b>Ваши билеты:</b> {tickets}\n"
        f"   └ реф-билеты в сезоне: {season_ref_tickets}/10\n"
        f"   └ билеты с колеса: {season_bonus_tickets}\n"
    )
    return msg


# --- START ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not IS_ACTIVE:
        pause_text = (
            "🏁 <b>РОЗЫГРЫШ ЗАВЕРШЕН!</b>\n\n"
            "Прямо сейчас мы подводим итоги и готовим новый сезон.\n"
            "Список каналов временно скрыт.\n\n"
            "🔔 <i>Ожидайте уведомления о старте нового конкурса!</i>"
        )
        await update.message.reply_text(pause_text, parse_mode=ParseMode.HTML)
        return

    user = update.effective_user
    uid = user.id
    first_name = user.first_name
    username = user.username

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (user_id, username)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET username = EXCLUDED.username
                    """,
                    (uid, username),
                )
                conn.commit()
    except Exception as e:
        print(f"Ошибка регистрации: {e}")

    season_id, season_start, season_end = get_active_season()
    ensure_user_season(uid, season_id)
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET last_seen=%s WHERE user_id=%s", (utcnow(), uid))
                conn.commit()
    except Exception as e:
        print("last_seen update error:", e)

    if context.args:
        ref_str = context.args[0]
        if ref_str.isdigit() and int(ref_str) != uid:
            referrer = int(ref_str)
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO referrals (referrer_id, referred_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (referrer, uid),
                        )

                        if cur.rowcount > 0:
                            cur.execute(
                                """
                                UPDATE users
                                SET lifetime_ref_count = lifetime_ref_count + 1
                                WHERE user_id = %s
                                RETURNING lifetime_ref_count, activated
                                """,
                                (referrer,),
                            )
                            row = cur.fetchone()
                            if row:
                                lr, activated = row
                                if (not activated) and lr >= 2:
                                    cur.execute("UPDATE users SET activated=TRUE WHERE user_id=%s", (referrer,))
                                    activated = True

                                if activated:
                                    ensure_user_season(referrer, season_id)

                                    cur.execute(
                                        """
                                        SELECT COALESCE(season_ref_tickets,0)
                                        FROM users
                                        WHERE user_id=%s
                                        FOR UPDATE
                                        """,
                                        (referrer,),
                                    )
                                    sref = cur.fetchone()[0]
                                    if sref < 10:
                                        cur.execute(
                                            """
                                            UPDATE users
                                            SET season_ref_tickets = season_ref_tickets + 1,
                                                tickets = tickets + 1
                                            WHERE user_id=%s
                                            """,
                                            (referrer,),
                                        )

                        conn.commit()
            except Exception as e:
                print(f"Ошибка рефералки: {e}")

    await update.message.reply_text(
        "Открой мини-приложение 'Колесо фортуны' кнопкой ниже:",
        reply_markup=get_fortune_shortcut(uid),
    )

    text = await get_start_text(uid, first_name, context)
    kb = [
        [InlineKeyboardButton("🔄 Проверить подписку", callback_data="check_sub")],
        [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [
            InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard"),
            InlineKeyboardButton("🏅 Победители", callback_data="winners_list"),
        ],
    ]
    await update.message.reply_text(
        text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb)
    )


# --- КНОПКИ ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if not IS_ACTIVE:
        await query.answer()
        await query.edit_message_text(
            "🏁 Розыгрыш завершен. Идет подготовка нового этапа.",
            parse_mode=ParseMode.HTML,
        )
        return

    uid = query.from_user.id
    data = query.data

    if data in ("check_sub", "back_to_main"):
        await query.answer("Обновляю...")
        text = await get_start_text(uid, query.from_user.first_name, context)
        kb = [
            [InlineKeyboardButton("🔄 Проверить подписку", callback_data="check_sub")],
            [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
            [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
            [
                InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard"),
                InlineKeyboardButton("🏅 Победители", callback_data="winners_list"),
            ],
        ]
        try:
            await query.edit_message_text(
                text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb)
            )
        except:
            pass

    elif data == "my_tickets":
        await query.answer()
        await get_start_text(uid, query.from_user.first_name, context)

        tickets = get_tickets(uid)

        is_sub = False
        activated = False
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(all_subscribed,0), COALESCE(activated,false) FROM users WHERE user_id=%s",
                        (uid,),
                    )
                    row = cur.fetchone()
                    if row:
                        is_sub = (row[0] == 1)
                        activated = bool(row[1])
        except:
            pass

        if not activated:
            text = (
                "⚠️ <b>Вы ещё не активированы.</b>\n\n"
                "Нужно пригласить <b>2</b> друзей (один раз), чтобы участвовать полноценно.\n"
                f"🎫 Сейчас билетов: <b>{tickets}</b>"
            )
        elif not is_sub:
            text = (
                "⚠️ <b>Вы не подписаны на спонсоров!</b>\n\n"
                "Для участия в розыгрыше нужно быть подписанным.\n"
                f"🎫 Ваши билеты сохранены: <b>{tickets}</b>"
            )
        else:
            text = f"🎫 <b>Ваши билеты: {tickets}</b>"

        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML
        )

    elif data == "my_reflink":
        await query.answer()
        link = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={uid}"
        text = (
            f"🔗 <b>Ваша ссылка для приглашения:</b>\n\n"
            f"<code>{link}</code>\n\n"
            f"Нужно <b>2</b> друга (один раз), чтобы активироваться."
        )
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "leaderboard":
        await query.answer()
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT username, tickets
                        FROM users
                        WHERE tickets > 0
                        ORDER BY tickets DESC
                        LIMIT 10
                        """
                    )
                    rows = cur.fetchall()

            if not rows:
                res = "Пока пусто."
            else:
                res = "🏆 <b>ТОП-10 ПО БИЛЕТАМ:</b>\n\n"
                for i, r in enumerate(rows, 1):
                    res += f"{i}. {mask_username(r[0])} — {r[1]} 🎫\n"
        except:
            res = "Ошибка."

        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(res, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "winners_list":
        await query.answer()
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT username, win_date FROM winners ORDER BY win_date DESC LIMIT 15"
                    )
                    rows = cur.fetchall()

            if not rows:
                res = "📜 Список победителей пока пуст."
            else:
                res = "🏅 <b>ПОСЛЕДНИЕ 15 ПОБЕДИТЕЛЕЙ:</b>\n\n"
                for i, r in enumerate(rows, 1):
                    safe_name = mask_username(r[0])
                    date_str = r[1].strftime("%d.%m.%Y") if r[1] else "-"
                    res += f"{i}. <b>{safe_name}</b> ({date_str})\n"
        except:
            res = "Ошибка."

        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(res, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

# --- АДМИНКА ---
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
            except:
                pass

        await update.message.reply_text(f"✅ Доставлено: {count}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    try:
        season_id, season_start, season_end = get_active_season()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                total_users = cur.fetchone()[0]

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM users
                    WHERE tickets > 0 AND all_subscribed = 1
                    """
                )
                active_participants = cur.fetchone()[0]

                cur.execute("SELECT COALESCE(SUM(tickets),0) FROM users")
                total_tickets = cur.fetchone()[0] or 0

        text = (
            f"📊 <b>СТАТИСТИКА БОТА:</b>\n\n"
            f"🗓 <b>Сезон:</b> {season_start.strftime('%d.%m.%Y')} — {season_end.strftime('%d.%m.%Y')}\n"
            f"👥 <b>Всего пользователей:</b> {total_users}\n"
            f"✅ <b>Активных участников (subscribed + tickets>0):</b> {active_participants}\n"
            f"🎫 <b>Всего билетов в игре (сезонный баланс):</b> {total_tickets}\n"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка получения статистики: {e}")


async def stop_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    global IS_ACTIVE
    IS_ACTIVE = False
    await update.message.reply_text("⛔️ <b>ПАУЗА</b>", parse_mode=ParseMode.HTML)


async def resume_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    global IS_ACTIVE
    IS_ACTIVE = True
    await update.message.reply_text("▶️ <b>СТАРТ</b>", parse_mode=ParseMode.HTML)


async def reset_season(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ручной сброс текущего сезона (вариант A): всем обнулить сезонные данные.
    Внимание: это не создаёт новый season в таблице seasons. Просто обнуляет всем.
    """
    if update.effective_user.id not in ADMINS:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET
                      tickets = 0,
                      season_ref_tickets = 0,
                      season_bonus_tickets = 0,
                      last_fortune_time = NULL,
                      season_id = NULL
                    """
                )
                conn.commit()
        await update.message.reply_text("✅ <b>Сезон сброшен!</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


# --- Колесо фортуны: обработка данных WebApp ---
async def handle_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    now = utcnow()

    try:
        data_str = update.effective_message.web_app_data.data
        parsed_data = json.loads(data_str)

        if parsed_data.get("action") != "spin_result":
            return

        prize_code = parsed_data.get("prize")

        prize_to_tickets = {
            "ticket_1": 1,
            "ticket_2": 2,
            "ticket_3": 3,
            "ticket_4": 4,
            "ticket_5": 5,
        }

        add_tickets = prize_to_tickets.get(prize_code, 0)

        if prize_code == "nothing":
            prize_text = "Увы, сектор «Ничего». Попробуй через 6 часов."
        elif add_tickets > 0:
            prize_text = f"🎉 Вы выиграли: <b>+{add_tickets} билет(ов)</b>!"
        else:
            prize_text = "❌ Неизвестный приз. Обновите колесо и попробуйте снова."

        if not prize_text:
            prize_text = "✅ Результат получен."

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT last_fortune_time FROM users WHERE user_id=%s", (user_id,))
                row = cur.fetchone()
                last_spin_time = row[0] if row else None

                # ВАЖНО: не делаем last_spin_time timezone-aware.
                # Просто приводим к naive (чтобы now - last_spin_time работало всегда).
                last_spin_time = to_naive_utc(last_spin_time)

                # кулдаун 6 часов
                if last_spin_time:
                    delta = now - last_spin_time
                    if delta < timedelta(hours=6):
                        seconds_left = int(timedelta(hours=6).total_seconds() - delta.total_seconds())
                        h_left = seconds_left // 3600
                        m_left = (seconds_left % 3600) // 60
                        await update.effective_message.reply_text(
                            f"⏳ Колесо заряжается! Ждите {h_left}ч {m_left}м.",
                            parse_mode=ParseMode.HTML,
                        )
                        return

                # начисление / фиксация времени
                if add_tickets > 0:
                    cur.execute(
                        """
                        UPDATE users
                        SET tickets = COALESCE(tickets,0) + %s,
                            season_bonus_tickets = COALESCE(season_bonus_tickets,0) + %s,
                            last_fortune_time = %s
                        WHERE user_id = %s
                        """,
                        (add_tickets, add_tickets, now, user_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE users
                        SET last_fortune_time = %s
                        WHERE user_id = %s
                        """,
                        (now, user_id),
                    )

                conn.commit()

        await update.effective_message.reply_text(prize_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        await update.effective_message.reply_text("❌ Ошибка обработки приза. Напишите админу.")
        import traceback
        print("Ошибка WebApp:", e)
        print(traceback.format_exc())


# --- DRAW (2 победителя) ---
async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, username, tickets
                    FROM users
                    WHERE tickets > 0 AND all_subscribed = 1
                    """
                )
                rows = cur.fetchall()

        if len(rows) < 2:
            await update.message.reply_text(
                f"❌ Недостаточно участников для выбора 2 победителей (нужно минимум 2, сейчас: {len(rows)})."
            )
            return

        pool = []
        for r in rows:
            pool.extend([r] * int(r[2]))

        if len(pool) < 2:
            await update.message.reply_text("❌ Недостаточно билетов для двух победителей.")
            return

        winner1 = random.choice(pool)
        pool2 = [p for p in pool if p[0] != winner1[0]]
        if not pool2:
            await update.message.reply_text("⚠️ Все билеты у одного участника. Второй победитель невозможен.")
            return
        winner2 = random.choice(pool2)

        winners = [winner1, winner2]

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    for wid, wname, wtickets in winners:
                        cur.execute(
                            "INSERT INTO winners (user_id, username, prize) VALUES (%s, %s, %s)",
                            (wid, wname, PRIZE),
                        )
                    conn.commit()
        except Exception as e:
            print(f"Ошибка сохранения победителей: {e}")

        result_msg = "🎉 <b>ПОБЕДИТЕЛИ РОЗЫГРЫША:</b>\n\n"
        for i, (wid, wname, wtickets) in enumerate(winners, 1):
            safe = f"@{wname}" if wname else "Нет ника"
            result_msg += f"{i}. {safe} (ID: <code>{wid}</code>) — {wtickets} 🎫\n"

        await update.message.reply_text(result_msg, parse_mode=ParseMode.HTML)

        win_msg = (
            f"🎉 <b>ПОЗДРАВЛЯЕМ! ВЫ ВЫИГРАЛИ!</b>\n\n"
            f"Приз: <b>{PRIZE}</b>\n\n"
            f"❗️ Свяжитесь с администратором для получения приза.\n"
            f"👉 <b>Написать:</b> @moderatorgive_bot\n\n"
            f"⏳ <b>Важно:</b> 48 часов на связь."
        )

        success_count = 0
        for wid, _, _ in winners:
            try:
                await context.bot.send_message(wid, win_msg, parse_mode=ParseMode.HTML)
                success_count += 1
            except:
                pass

        await update.message.reply_text(f"✅ ЛС отправлено {success_count} из 2 победителям.")

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка в /draw: {e}")
        import traceback
        print(traceback.format_exc())


async def fortune(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Жми на кнопку ниже и лови призы!",
        reply_markup=get_fortune_shortcut(update.effective_user.id),
    )


def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))

    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("draw", draw))
    app.add_handler(CommandHandler("stop", stop_giveaway))
    app.add_handler(CommandHandler("resume", resume_giveaway))
    app.add_handler(CommandHandler("reset_season", reset_season))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("fortune", fortune))

    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data))

    print("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()
