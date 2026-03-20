#!/usr/bin/env python3
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("MY_DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("No MY_DATABASE_URL/DATABASE_URL in environment")

COOLDOWN_HOURS = 6
SPIN_COST_STARS = 2

BASE_WEIGHTS = {
    "nothing": 50.0,
    "star_1": 20.0,
    "star_2": 15.0,
    "star_3": 7.0,
    "star_4": 5.0,
    "star_5": 3.0,
}

PRIZE_TO_STARS = {
    "star_1": 2,
    "star_2": 4,
    "star_3": 6,
    "star_4": 8,
    "star_5": 10,
}

LABEL_MAP = {
    "nothing": "Ничего",
    "star_1": "+2 звезды",
    "star_2": "+4 звезды",
    "star_3": "+6 звёзд",
    "star_4": "+8 звёзд",
    "star_5": "+10 звёзд",
}


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_naive_utc(dt):
    if dt is None:
        return None
    return dt.replace(tzinfo=None)


def get_level_info(ref_count: int):
    if ref_count >= 15:
        return {
            "name": "Diamond",
            "emoji": "🌟",
            "bonus_percent": 80,
            "multiplier": 1.80,
        }
    if ref_count >= 10:
        return {
            "name": "Gold",
            "emoji": "🥇",
            "bonus_percent": 45,
            "multiplier": 1.45,
        }
    if ref_count >= 5:
        return {
            "name": "Silver",
            "emoji": "🥈",
            "bonus_percent": 20,
            "multiplier": 1.20,
        }
    return {
        "name": "Bronze",
        "emoji": "🥉",
        "bonus_percent": 0,
        "multiplier": 1.00,
    }


def get_wheel_weights_by_bonus(total_bonus_percent: int):
    mult = 1.0 + (float(total_bonus_percent) / 100.0)

    boosted = {}
    boosted_sum = 0.0

    for key in ["star_1", "star_2", "star_3", "star_4", "star_5"]:
        boosted[key] = round(BASE_WEIGHTS[key] * mult, 2)
        boosted_sum += boosted[key]

    nothing_weight = round(100.0 - boosted_sum, 2)
    if nothing_weight < 0:
        nothing_weight = 0.0

    boosted["nothing"] = nothing_weight
    return boosted


def get_active_sponsors(cur, include_temp=True):
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
    sponsors = []
    for slot_no, sponsor_type, channel_username in rows:
        sponsors.append(
            {
                "slot_no": slot_no,
                "sponsor_type": sponsor_type,
                "channel_username": channel_username,
            }
        )
    return sponsors


def get_user_state(cur, user_id: int):
    cur.execute(
        """
        SELECT
            COALESCE(tickets, 0),
            last_fortune_time,
            COALESCE(all_subscribed, 0),
            COALESCE(activated, FALSE),
            COALESCE(active_ref_count, 0),
            COALESCE(activation_bonus_percent, 0),
            COALESCE(boost_percent, 0),
            COALESCE(boost_spins_left, 0),
            COALESCE(paid_spins, 0)
        FROM users
        WHERE user_id = %s
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        return None

    (
        stars,
        last_time,
        all_subscribed,
        activated,
        ref_count,
        activation_bonus_percent,
        boost_percent,
        boost_spins_left,
        paid_spins,
    ) = row
    return {
        "stars": int(stars or 0),
        "last_time": to_naive_utc(last_time),
        "all_subscribed": int(all_subscribed or 0),
        "activated": bool(activated),
        "ref_count": int(ref_count or 0),
        "activation_bonus_percent": int(activation_bonus_percent or 0),
        "boost_percent": int(boost_percent or 0),
        "boost_spins_left": int(boost_spins_left or 0),
        "paid_spins": int(paid_spins or 0),
    }


@app.get("/api/is_can_spin")
def is_can_spin():
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify({"ok": False, "error": "missing_user_id"}), 400

    now = now_utc()
    cooldown = timedelta(hours=COOLDOWN_HOURS)

    with get_conn() as conn:
        with conn.cursor() as cur:
            state = get_user_state(cur, user_id)
            if state is None:
                return jsonify({"ok": False, "error": "user_not_found"}), 404

            sponsors = get_active_sponsors(cur, include_temp=True)
            main_sponsors = get_active_sponsors(cur, include_temp=False)

            level = get_level_info(state["ref_count"])
            total_bonus_percent = int(state.get("activation_bonus_percent", 0)) + int(level["bonus_percent"]) + int(state.get("boost_percent", 0))
            weights = get_wheel_weights_by_bonus(total_bonus_percent)

            if not main_sponsors:
                return jsonify(
                    {
                        "ok": True,
                        "can_spin": False,
                        "can_buy_spin": False,
                        "spin_cost": SPIN_COST_STARS,
                        "stars": state["stars"],
                        "reason": "not_configured",
                        "message": "Основные спонсоры ещё не настроены.",
                        "level": level["name"],
                        "level_emoji": level["emoji"],
                        "bonus_percent": level["bonus_percent"],
                        "activation_bonus_percent": state.get("activation_bonus_percent", 0),
                        "boost_percent": state.get("boost_percent", 0),
                        "boost_spins_left": state.get("boost_spins_left", 0),
                        "paid_spins": state.get("paid_spins", 0),
                        "total_bonus_percent": total_bonus_percent,
                        "weights": weights,
                    }
                )

            if state["all_subscribed"] != 1:
                sponsor_names = ", ".join([s["channel_username"] for s in sponsors]) if sponsors else "активных спонсоров"
                return jsonify(
                    {
                        "ok": True,
                        "can_spin": False,
                        "can_buy_spin": False,
                        "spin_cost": SPIN_COST_STARS,
                        "stars": state["stars"],
                        "reason": "not_subscribed",
                        "message": f"Подпишитесь на всех активных спонсоров для доступа к Звёздному Колесу: {sponsor_names}",
                        "level": level["name"],
                        "level_emoji": level["emoji"],
                        "bonus_percent": level["bonus_percent"],
                        "activation_bonus_percent": state.get("activation_bonus_percent", 0),
                        "boost_percent": state.get("boost_percent", 0),
                        "boost_spins_left": state.get("boost_spins_left", 0),
                        "paid_spins": state.get("paid_spins", 0),
                        "total_bonus_percent": total_bonus_percent,
                        "weights": weights,
                    }
                )

            if not state["activated"]:
                return jsonify(
                    {
                        "ok": True,
                        "can_spin": False,
                        "can_buy_spin": False,
                        "spin_cost": SPIN_COST_STARS,
                        "stars": state["stars"],
                        "reason": "not_activated",
                        "message": "Пригласите 2 активных реферала для открытия Звёздного Колеса.",
                        "level": level["name"],
                        "level_emoji": level["emoji"],
                        "bonus_percent": level["bonus_percent"],
                        "activation_bonus_percent": state.get("activation_bonus_percent", 0),
                        "boost_percent": state.get("boost_percent", 0),
                        "boost_spins_left": state.get("boost_spins_left", 0),
                        "paid_spins": state.get("paid_spins", 0),
                        "total_bonus_percent": total_bonus_percent,
                        "weights": weights,
                    }
                )

            last_time = state["last_time"]

            if last_time is None:
                return jsonify(
                    {
                        "ok": True,
                        "can_spin": True,
                        "can_buy_spin": state["stars"] >= SPIN_COST_STARS,
                        "spin_cost": SPIN_COST_STARS,
                        "stars": state["stars"],
                        "paid_spins": state.get("paid_spins", 0),
                        "paid_spins": state.get("paid_spins", 0),
                        "wait_seconds": 0,
                        "wait_msg": "",
                        "reason": None,
                        "message": "Можно крутить прямо сейчас.",
                        "level": level["name"],
                        "level_emoji": level["emoji"],
                        "bonus_percent": level["bonus_percent"],
                        "activation_bonus_percent": state.get("activation_bonus_percent", 0),
                        "boost_percent": state.get("boost_percent", 0),
                        "boost_spins_left": state.get("boost_spins_left", 0),
                        "paid_spins": state.get("paid_spins", 0),
                        "total_bonus_percent": total_bonus_percent,
                        "weights": weights,
                    }
                )

            delta = now - last_time
            if delta >= cooldown:
                return jsonify(
                    {
                        "ok": True,
                        "can_spin": True,
                        "can_buy_spin": state["stars"] >= SPIN_COST_STARS,
                        "spin_cost": SPIN_COST_STARS,
                        "stars": state["stars"],
                        "paid_spins": state.get("paid_spins", 0),
                        "paid_spins": state.get("paid_spins", 0),
                        "wait_seconds": 0,
                        "wait_msg": "",
                        "reason": None,
                        "message": "Можно крутить прямо сейчас.",
                        "level": level["name"],
                        "level_emoji": level["emoji"],
                        "bonus_percent": level["bonus_percent"],
                        "activation_bonus_percent": state.get("activation_bonus_percent", 0),
                        "boost_percent": state.get("boost_percent", 0),
                        "boost_spins_left": state.get("boost_spins_left", 0),
                        "paid_spins": state.get("paid_spins", 0),
                        "total_bonus_percent": total_bonus_percent,
                        "weights": weights,
                    }
                )

            remaining = cooldown - delta
            wait_seconds = max(0, int(remaining.total_seconds()))
            hours = wait_seconds // 3600
            minutes = (wait_seconds % 3600) // 60

            return jsonify(
                {
                    "ok": True,
                    "can_spin": state.get("paid_spins", 0) > 0,
                    "can_buy_spin": state["stars"] >= SPIN_COST_STARS,
                    "spin_cost": SPIN_COST_STARS,
                    "stars": state["stars"],
                    "paid_spins": state.get("paid_spins", 0),
                    "wait_seconds": wait_seconds,
                    "wait_msg": f"Подождите {hours}ч {minutes}м до следующего бесплатного вращения.",
                    "reason": "cooldown",
                    "message": "Колесо перезаряжается.",
                    "level": level["name"],
                    "level_emoji": level["emoji"],
                    "bonus_percent": level["bonus_percent"],
                    "activation_bonus_percent": state.get("activation_bonus_percent", 0),
                    "boost_percent": state.get("boost_percent", 0),
                    "boost_spins_left": state.get("boost_spins_left", 0),
                    "total_bonus_percent": total_bonus_percent,
                    "weights": weights,
                }
            )


@app.post("/api/buy_spin")
def buy_spin():
    data = request.get_json(silent=True) or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "error": "missing_user_id"}), 400

    user_id = int(user_id)
    now = now_utc()
    cooldown = timedelta(hours=COOLDOWN_HOURS)

    with get_conn() as conn:
        with conn.cursor() as cur:
            state = get_user_state(cur, user_id)
            if state is None:
                return jsonify({"ok": False, "error": "user_not_found"}), 404

            sponsors = get_active_sponsors(cur, include_temp=True)
            main_sponsors = get_active_sponsors(cur, include_temp=False)

            if not main_sponsors:
                return jsonify(
                    {
                        "ok": True,
                        "success": False,
                        "error": "not_configured",
                        "message": "Основные спонсоры ещё не настроены.",
                        "stars": state["stars"],
                        "spin_cost": SPIN_COST_STARS,
                    }
                )

            if state["all_subscribed"] != 1:
                sponsor_names = ", ".join([s["channel_username"] for s in sponsors]) if sponsors else "активных спонсоров"
                return jsonify(
                    {
                        "ok": True,
                        "success": False,
                        "error": "not_subscribed",
                        "message": f"Нет подписки на всех активных спонсоров: {sponsor_names}",
                        "stars": state["stars"],
                        "spin_cost": SPIN_COST_STARS,
                    }
                )

            if not state["activated"]:
                return jsonify(
                    {
                        "ok": True,
                        "success": False,
                        "error": "not_activated",
                        "message": "Нет 2 активных рефералов.",
                        "stars": state["stars"],
                        "spin_cost": SPIN_COST_STARS,
                    }
                )

            last_time = state["last_time"]

            if state["stars"] < SPIN_COST_STARS:
                return jsonify(
                    {
                        "ok": True,
                        "success": False,
                        "error": "not_enough_stars",
                        "message": "Недостаточно звёзд для покупки спина.",
                        "stars": state["stars"],
                        "spin_cost": SPIN_COST_STARS,
                    }
                )

            if last_time is None or (now - last_time) >= cooldown:
                return jsonify(
                    {
                        "ok": True,
                        "success": False,
                        "error": "cooldown_passed_spin_free",
                        "message": "Покупка не нужна — бесплатный спин уже доступен.",
                        "stars": state["stars"],
                        "spin_cost": SPIN_COST_STARS,
                    }
                )

            cur.execute(
                """
                UPDATE users
                SET tickets = tickets - %s,
                    paid_spins = COALESCE(paid_spins, 0) + 1
                WHERE user_id = %s
                RETURNING tickets, paid_spins
                """,
                (SPIN_COST_STARS, user_id),
            )
            row = cur.fetchone()
            new_stars = row[0]
            new_paid_spins = row[1]
            conn.commit()

            return jsonify(
                {
                    "ok": True,
                    "success": True,
                    "stars": int(new_stars or 0),
                    "paid_spins": int(new_paid_spins or 0),
                    "spin_cost": SPIN_COST_STARS,
                    "message": "Дополнительный спин успешно куплен.",
                }
            )


@app.post("/api/spin")
def spin():
    data = request.get_json(silent=True) or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "error": "missing_user_id"}), 400

    user_id = int(user_id)
    now = now_utc()
    cooldown = timedelta(hours=COOLDOWN_HOURS)

    with get_conn() as conn:
        with conn.cursor() as cur:
            state = get_user_state(cur, user_id)
            if state is None:
                return jsonify({"ok": False, "error": "user_not_found"}), 404

            sponsors = get_active_sponsors(cur, include_temp=True)
            main_sponsors = get_active_sponsors(cur, include_temp=False)

            if not main_sponsors:
                return jsonify(
                    {
                        "ok": False,
                        "error": "not_configured",
                        "message": "Основные спонсоры ещё не настроены.",
                    }
                ), 200

            if state["all_subscribed"] != 1:
                sponsor_names = ", ".join([s["channel_username"] for s in sponsors]) if sponsors else "активных спонсоров"
                return jsonify(
                    {
                        "ok": False,
                        "error": "not_subscribed",
                        "message": f"Нет подписки на всех активных спонсоров: {sponsor_names}",
                    }
                ), 200

            if not state["activated"]:
                return jsonify(
                    {
                        "ok": False,
                        "error": "not_activated",
                        "message": "Не выполнено условие по 2 активным рефералам.",
                    }
                ), 200

            last_time = state["last_time"]
            free_spin_ready = (last_time is None) or ((now - last_time) >= cooldown)
            use_paid_spin = False

            if not free_spin_ready:
                if state.get("paid_spins", 0) > 0:
                    use_paid_spin = True
                else:
                    remaining = cooldown - (now - last_time)
                    wait_seconds = max(0, int(remaining.total_seconds()))
                    return jsonify(
                        {
                            "ok": False,
                            "error": "cooldown",
                            "wait_seconds": wait_seconds,
                            "paid_spins": state.get("paid_spins", 0),
                        }
                    ), 200

            level = get_level_info(state["ref_count"])
            total_bonus_percent = int(state.get("activation_bonus_percent", 0)) + int(level["bonus_percent"]) + int(state.get("boost_percent", 0))
            weights_dict = get_wheel_weights_by_bonus(total_bonus_percent)

            codes = ["nothing", "star_1", "star_2", "star_3", "star_4", "star_5"]
            weights = [
                weights_dict["nothing"],
                weights_dict["star_1"],
                weights_dict["star_2"],
                weights_dict["star_3"],
                weights_dict["star_4"],
                weights_dict["star_5"],
            ]

            prize_code = random.choices(codes, weights=weights, k=1)[0]
            base_stars = PRIZE_TO_STARS.get(prize_code, 0)
            add_stars = base_stars
            spin_id = str(uuid.uuid4())

            cur.execute(
                """
                INSERT INTO fortune_spins (spin_id, user_id, prize_code, created_at)
                VALUES (%s, %s, %s, %s)
                """,
                (spin_id, user_id, prize_code, now),
            )

            new_boost_spins_left = int(state.get("boost_spins_left", 0))
            new_boost_percent = int(state.get("boost_percent", 0))

            if new_boost_spins_left > 0:
                new_boost_spins_left -= 1
                if new_boost_spins_left <= 0:
                    new_boost_spins_left = 0
                    new_boost_percent = 0

            if use_paid_spin:
                if add_stars > 0:
                    cur.execute(
                        """
                        UPDATE users
                        SET tickets = COALESCE(tickets, 0) + %s,
                            paid_spins = COALESCE(paid_spins, 0) - 1,
                            boost_percent = %s,
                            boost_spins_left = %s
                        WHERE user_id = %s
                        RETURNING tickets, paid_spins
                        """,
                        (add_stars, new_boost_percent, new_boost_spins_left, user_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE users
                        SET paid_spins = COALESCE(paid_spins, 0) - 1,
                            boost_percent = %s,
                            boost_spins_left = %s
                        WHERE user_id = %s
                        RETURNING tickets, paid_spins
                        """,
                        (new_boost_percent, new_boost_spins_left, user_id),
                    )
            else:
                if add_stars > 0:
                    cur.execute(
                        """
                        UPDATE users
                        SET tickets = COALESCE(tickets, 0) + %s,
                            last_fortune_time = %s,
                            boost_percent = %s,
                            boost_spins_left = %s
                        WHERE user_id = %s
                        RETURNING tickets, paid_spins
                        """,
                        (add_stars, now, new_boost_percent, new_boost_spins_left, user_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE users
                        SET last_fortune_time = %s,
                            boost_percent = %s,
                            boost_spins_left = %s
                        WHERE user_id = %s
                        RETURNING tickets, paid_spins
                        """,
                        (now, new_boost_percent, new_boost_spins_left, user_id),
                    )

            row = cur.fetchone()
            new_stars = row[0]
            new_paid_spins = row[1]
            conn.commit()

    return jsonify(
        {
            "ok": True,
            "spin_id": spin_id,
            "prize": prize_code,
            "label": LABEL_MAP.get(prize_code, prize_code),
            "add_stars": add_stars,
            "stars": int(new_stars or 0),
            "level": level["name"],
            "level_emoji": level["emoji"],
            "bonus_percent": level["bonus_percent"],
            "activation_bonus_percent": state.get("activation_bonus_percent", 0),
            "boost_percent": new_boost_percent,
            "boost_spins_left": new_boost_spins_left,
            "paid_spins": int(new_paid_spins or 0),
            "total_bonus_percent": total_bonus_percent,
            "weights": weights_dict,
        }
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000)
