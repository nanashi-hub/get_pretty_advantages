from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Optional, Set

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, case, func
from sqlalchemy.orm import Session

from app.auth import get_current_user
from app.database import get_db
from app.models import EarningRecord, User, UserReferral, UserRole, UserScriptConfig, UserScriptEnv
from app.schemas import EarningRecordCreate, EarningRecordResponse

router = APIRouter(prefix="/api", tags=["收益管理"])

COINS_PER_YUAN = 10000
DEFAULT_PERIOD_DAYS = 7  # 默认近7天（含今日）
INCOME_RATE_ME_PCT = 100
INCOME_RATE_L1_PCT = 20
INCOME_RATE_L2_PCT = 4


def _unique_in_order(values: List[int]) -> List[int]:
    seen: Set[int] = set()
    result: List[int] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _coins_to_yuan(coins: int) -> float:
    return round((coins or 0) / COINS_PER_YUAN, 2)


def _income_coins(coins: int, rate_pct: int) -> int:
    if coins <= 0 or rate_pct <= 0:
        return 0
    return int(coins) * int(rate_pct) // 100


def _income_total_coins(me_coins: int, l1_coins: int, l2_coins: int) -> int:
    return (
        _income_coins(int(me_coins or 0), INCOME_RATE_ME_PCT)
        + _income_coins(int(l1_coins or 0), INCOME_RATE_L1_PCT)
        + _income_coins(int(l2_coins or 0), INCOME_RATE_L2_PCT)
    )


def _get_owned_env_ids(db: Session, user_id: int) -> List[int]:
    return [
        int(env_id)
        for (env_id,) in db.query(UserScriptEnv.id)
        .join(UserScriptConfig, UserScriptEnv.config_id == UserScriptConfig.id)
        .filter(UserScriptConfig.user_id == user_id)
        .all()
    ]


def _assert_env_belongs_to_user(db: Session, env_id: int, user_id: int) -> None:
    owned = (
        db.query(UserScriptEnv.id)
        .join(UserScriptConfig, UserScriptEnv.config_id == UserScriptConfig.id)
        .filter(UserScriptEnv.id == env_id, UserScriptConfig.user_id == user_id)
        .first()
    )
    if not owned:
        raise HTTPException(status_code=403, detail="无权操作该账号的收益记录")


def _get_descendant_user_ids(db: Session, my_user_id: int) -> Dict[str, List[int]]:
    """返回当前用户的下级用户ID（+1/+2）"""
    level1_user_ids = [
        int(user_id)
        for (user_id,) in db.query(UserReferral.user_id)
        .filter(UserReferral.inviter_level1 == my_user_id)
        .all()
    ]
    level2_user_ids = [
        int(user_id)
        for (user_id,) in db.query(UserReferral.user_id)
        .filter(UserReferral.inviter_level2 == my_user_id)
        .all()
    ]
    return {"l1": level1_user_ids, "l2": level2_user_ids}


def _get_env_ids_for_users(db: Session, user_ids: List[int]) -> List[int]:
    """按 user_script_envs.user_id 取账号 env_id 列表（一个用户可有多个账号）"""
    if not user_ids:
        return []
    rows = db.query(UserScriptEnv.id).filter(UserScriptEnv.user_id.in_(user_ids)).all()
    return [int(env_id) for (env_id,) in rows]


def _get_env_remark_map(db: Session, env_ids: List[int]) -> Dict[int, str]:
    if not env_ids:
        return {}
    rows = (
        db.query(UserScriptEnv.id, UserScriptEnv.remark)
        .filter(UserScriptEnv.id.in_(env_ids))
        .all()
    )
    return {int(env_id): (remark or "") for env_id, remark in rows}


def _classify_status(today_coins: int, avg7_coins: float) -> Dict[str, str]:
    if today_coins <= 0:
        return {"status": "no_earnings", "explanation": "今日无收益"}
    if avg7_coins > 0 and today_coins < avg7_coins * 0.5:
        drop_pct = (1 - (today_coins / avg7_coins)) * 100
        return {"status": "abnormal", "explanation": f"低于近7日均值 {drop_pct:.0f}%"}
    return {"status": "normal", "explanation": "表现稳定"}


def _get_accounts_today_and_avg7(
    db: Session,
    env_ids: List[int],
    today: date,
) -> Dict[int, dict]:
    if not env_ids:
        return {}

    yesterday = today - timedelta(days=1)
    day7 = today - timedelta(days=7)

    today_coins_expr = func.coalesce(
        func.sum(
            case(
                (EarningRecord.stat_date == today, EarningRecord.coins_total),
                else_=0,
            )
        ),
        0,
    )
    avg7_coins_expr = func.coalesce(
        func.avg(
            case(
                (
                    and_(EarningRecord.stat_date >= day7, EarningRecord.stat_date <= yesterday),
                    EarningRecord.coins_total,
                ),
                else_=None,
            )
        ),
        0,
    )

    today_box_expr = func.coalesce(
        func.sum(case((EarningRecord.stat_date == today, EarningRecord.coins_from_box), else_=0)),
        0,
    )
    today_look_expr = func.coalesce(
        func.sum(case((EarningRecord.stat_date == today, EarningRecord.coins_from_look), else_=0)),
        0,
    )
    today_food_expr = func.coalesce(
        func.sum(case((EarningRecord.stat_date == today, EarningRecord.coins_from_food), else_=0)),
        0,
    )
    today_search_expr = func.coalesce(
        func.sum(case((EarningRecord.stat_date == today, EarningRecord.coins_from_search), else_=0)),
        0,
    )

    rows = (
        db.query(
            EarningRecord.env_id.label("env_id"),
            today_coins_expr.label("today_coins"),
            avg7_coins_expr.label("avg7_coins"),
            today_box_expr.label("coins_from_box"),
            today_look_expr.label("coins_from_look"),
            today_food_expr.label("coins_from_food"),
            today_search_expr.label("coins_from_search"),
        )
        .filter(
            EarningRecord.env_id.in_(env_ids),
            EarningRecord.stat_date.between(day7, today),
        )
        .group_by(EarningRecord.env_id)
        .all()
    )

    result: Dict[int, dict] = {int(env_id): {"env_id": int(env_id)} for env_id in env_ids}
    for row in rows:
        env_id = int(row.env_id)
        result[env_id] = {
            "env_id": env_id,
            "today_coins": int(row.today_coins or 0),
            "avg7_coins": float(row.avg7_coins or 0),
            "sources": {
                "box": int(row.coins_from_box or 0),
                "look": int(row.coins_from_look or 0),
                "food": int(row.coins_from_food or 0),
                "search": int(row.coins_from_search or 0),
            },
        }
    for env_id, payload in result.items():
        payload.setdefault("today_coins", 0)
        payload.setdefault("avg7_coins", 0.0)
        payload.setdefault("sources", {"box": 0, "look": 0, "food": 0, "search": 0})
        payload.update(_classify_status(payload["today_coins"], payload["avg7_coins"]))
    return result


@router.get("/earnings", response_model=List[EarningRecordResponse])
async def get_earnings(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    env_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """获取收益记录（按 env_id + stat_date）"""
    query = db.query(EarningRecord)

    if current_user.role != UserRole.ADMIN:
        owned_env_ids = _get_owned_env_ids(db, current_user.id)
        if not owned_env_ids:
            return []
        query = query.filter(EarningRecord.env_id.in_(owned_env_ids))

    if start_date:
        query = query.filter(EarningRecord.stat_date >= start_date)
    if end_date:
        query = query.filter(EarningRecord.stat_date <= end_date)
    if env_id:
        query = query.filter(EarningRecord.env_id == env_id)

    return query.order_by(EarningRecord.stat_date.desc()).all()


@router.get("/stats/earnings")
async def get_earnings_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """收益统计（兼容旧页面接口：总/今日/本周）"""
    today = date.today()
    week_ago = today - timedelta(days=7)

    base_query = db.query(EarningRecord)
    if current_user.role != UserRole.ADMIN:
        owned_env_ids = _get_owned_env_ids(db, current_user.id)
        if not owned_env_ids:
            return {"total_coins": 0, "today_coins": 0, "week_coins": 0, "estimated_amount": 0.0}
        base_query = base_query.filter(EarningRecord.env_id.in_(owned_env_ids))

    total_coins = int(base_query.with_entities(func.coalesce(func.sum(EarningRecord.coins_total), 0)).scalar() or 0)
    today_coins = int(
        base_query.filter(EarningRecord.stat_date == today)
        .with_entities(func.coalesce(func.sum(EarningRecord.coins_total), 0))
        .scalar()
        or 0
    )
    week_coins = int(
        base_query.filter(EarningRecord.stat_date >= week_ago)
        .with_entities(func.coalesce(func.sum(EarningRecord.coins_total), 0))
        .scalar()
        or 0
    )

    return {
        "total_coins": total_coins,
        "today_coins": today_coins,
        "week_coins": week_coins,
        "estimated_amount": _coins_to_yuan(total_coins),
    }


@router.get("/stats/earnings-hierarchy")
async def get_earnings_hierarchy(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    按真实结构返回收益统计（含我 + 一级(-1) + 二级(-2)）。

    约定：
    - user_referrals.user_id / inviter_level1 / inviter_level2 均为 users.id（用户主键）
    - 账号实体为 user_script_envs（env_id = user_script_envs.id），账号展示名来自 user_script_envs.remark
    - earning_records 记录的是金币；金额=coins/10000
    """
    as_of_date = end_date or date.today()
    end_date = as_of_date
    if start_date is None:
        start_date = end_date - timedelta(days=DEFAULT_PERIOD_DAYS - 1)
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="起始日期不能大于结束日期")

    my_user_id = int(current_user.id)
    my_display_name = current_user.nickname or current_user.username or f"用户#{my_user_id}"

    descendants = _get_descendant_user_ids(db, my_user_id)
    l1_user_ids = _unique_in_order(descendants["l1"])
    l2_user_ids = _unique_in_order(descendants["l2"])

    # 防御性处理：避免脏数据导致 L1/L2 重叠或包含自己，从而重复统计/重复展示
    l1_user_ids = [uid for uid in l1_user_ids if uid != my_user_id]
    l1_set = set(l1_user_ids)
    l2_user_ids = [uid for uid in l2_user_ids if uid != my_user_id and uid not in l1_set]

    my_env_ids = _get_env_ids_for_users(db, [my_user_id])
    l1_env_ids = _get_env_ids_for_users(db, l1_user_ids)
    l2_env_ids = _get_env_ids_for_users(db, l2_user_ids)
    scope_env_ids = _unique_in_order(my_env_ids + l1_env_ids + l2_env_ids)

    yesterday = as_of_date - timedelta(days=1)

    def sum_coins(env_ids: List[int], from_date: date, to_date: date) -> int:
        if not env_ids:
            return 0
        return int(
            db.query(func.coalesce(func.sum(EarningRecord.coins_total), 0))
            .filter(
                EarningRecord.env_id.in_(env_ids),
                EarningRecord.stat_date.between(from_date, to_date),
            )
            .scalar()
            or 0
        )

    remark_map = _get_env_remark_map(db, scope_env_ids)
    account_stats = _get_accounts_today_and_avg7(db, scope_env_ids, as_of_date)

    def build_account_rows(env_ids: List[int]) -> List[dict]:
        rows: List[dict] = []
        for env_id in env_ids:
            stat = account_stats.get(env_id, {"today_coins": 0, "avg7_coins": 0.0, "sources": {}})
            remark = remark_map.get(env_id) or f"账号#{env_id}"
            today_coins = int(stat.get("today_coins", 0))
            rows.append(
                {
                    "env_id": env_id,
                    "remark": remark,
                    "today_coins": today_coins,
                    "today_yuan": _coins_to_yuan(today_coins),
                    "avg7_coins": float(stat.get("avg7_coins", 0.0)),
                    "status": stat.get("status", "no_earnings"),
                    "explanation": stat.get("explanation", "今日无收益"),
                    "sources": stat.get("sources", {"box": 0, "look": 0, "food": 0, "search": 0}),
                }
            )
        return rows

    l1_accounts = build_account_rows(l1_env_ids)
    l2_accounts = build_account_rows(l2_env_ids)

    me_today_coins = sum(int(account_stats.get(env_id, {}).get("today_coins", 0)) for env_id in my_env_ids)
    l1_today_coins = sum(a["today_coins"] for a in l1_accounts)
    l2_today_coins = sum(a["today_coins"] for a in l2_accounts)

    me_yesterday_coins = sum_coins(my_env_ids, yesterday, yesterday)
    l1_yesterday_coins = sum_coins(l1_env_ids, yesterday, yesterday)
    l2_yesterday_coins = sum_coins(l2_env_ids, yesterday, yesterday)

    me_period_coins = sum_coins(my_env_ids, start_date, end_date)
    l1_period_coins = sum_coins(l1_env_ids, start_date, end_date)
    l2_period_coins = sum_coins(l2_env_ids, start_date, end_date)

    me_period_income_coins = _income_coins(me_period_coins, INCOME_RATE_ME_PCT)
    l1_period_income_coins = _income_coins(l1_period_coins, INCOME_RATE_L1_PCT)
    l2_period_income_coins = _income_coins(l2_period_coins, INCOME_RATE_L2_PCT)
    total_period_income_coins = me_period_income_coins + l1_period_income_coins + l2_period_income_coins

    today_total_income_coins = _income_total_coins(me_today_coins, l1_today_coins, l2_today_coins)
    yesterday_total_income_coins = _income_total_coins(me_yesterday_coins, l1_yesterday_coins, l2_yesterday_coins)
    period_total_income_coins = _income_total_coins(me_period_coins, l1_period_coins, l2_period_coins)

    today_total_gross_coins = me_today_coins + l1_today_coins + l2_today_coins
    period_total_gross_coins = me_period_coins + l1_period_coins + l2_period_coins

    def share_pct(part: int) -> float:
        if total_period_income_coins <= 0:
            return 0.0
        return round(part / total_period_income_coins * 100, 2)

    def abnormal_count(rows: List[dict]) -> int:
        return sum(1 for row in rows if row.get("status") != "normal")

    pct_change = None
    if yesterday_total_income_coins > 0:
        pct_change = round(
            (today_total_income_coins - yesterday_total_income_coins) / yesterday_total_income_coins * 100,
            2,
        )

    return {
        "coins_per_yuan": COINS_PER_YUAN,
        "period": {"start_date": str(start_date), "end_date": str(end_date)},
        "viewer": {
            "my_user_id": my_user_id,
            "my_display_name": my_display_name,
            "my_env_count": len(my_env_ids),
            # 兼容旧前端字段（避免历史缓存/老页面报错）
            "my_env_id": my_user_id,
            "my_remark": my_display_name,
        },
        "today_overview": {
            "total_coins": today_total_income_coins,
            "total_yuan": _coins_to_yuan(today_total_income_coins),
            "gross_total_coins": today_total_gross_coins,
            "yesterday_coins": yesterday_total_income_coins,
            "yesterday_yuan": _coins_to_yuan(yesterday_total_income_coins),
            "pct_change": pct_change,
        },
        "period_overview": {
            "total_coins": period_total_income_coins,
            "total_yuan": _coins_to_yuan(period_total_income_coins),
            "gross_total_coins": period_total_gross_coins,
        },
        "layers": {
            "me": {
                "user_id": my_user_id,
                "remark": my_display_name,
                "account_count": len(my_env_ids),
                "today_coins": me_today_coins,
                "today_yuan": _coins_to_yuan(me_today_coins),
                "period_coins": me_period_coins,
                "period_yuan": _coins_to_yuan(me_period_coins),
                "period_income_coins": me_period_income_coins,
                "period_income_yuan": _coins_to_yuan(me_period_income_coins),
                "share_pct": share_pct(me_period_income_coins),
            },
            "l1": {
                "count": len(l1_env_ids),
                "today_coins": l1_today_coins,
                "today_yuan": _coins_to_yuan(l1_today_coins),
                "period_coins": l1_period_coins,
                "period_yuan": _coins_to_yuan(l1_period_coins),
                "period_income_coins": l1_period_income_coins,
                "period_income_yuan": _coins_to_yuan(l1_period_income_coins),
                "share_pct": share_pct(l1_period_income_coins),
                "abnormal_count": abnormal_count(l1_accounts),
            },
            "l2": {
                "count": len(l2_env_ids),
                "today_coins": l2_today_coins,
                "today_yuan": _coins_to_yuan(l2_today_coins),
                "period_coins": l2_period_coins,
                "period_yuan": _coins_to_yuan(l2_period_coins),
                "period_income_coins": l2_period_income_coins,
                "period_income_yuan": _coins_to_yuan(l2_period_income_coins),
                "share_pct": share_pct(l2_period_income_coins),
                "abnormal_count": abnormal_count(l2_accounts),
            },
        },
        "accounts": {"l1": l1_accounts, "l2": l2_accounts},
    }


@router.get("/stats/earnings-trend")
async def get_earnings_trend(
    days: int = Query(30, ge=1, le=180),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """近 N 天金币趋势（我 / 一级下级 / 二级下级），按 users.id 维度统计。"""
    today = date.today()
    start_date = today - timedelta(days=days - 1)

    my_user_id = int(current_user.id)
    descendants = _get_descendant_user_ids(db, my_user_id)
    l1_user_ids = _unique_in_order(descendants["l1"])
    l2_user_ids = _unique_in_order(descendants["l2"])

    l1_user_ids = [uid for uid in l1_user_ids if uid != my_user_id]
    l1_set = set(l1_user_ids)
    l2_user_ids = [uid for uid in l2_user_ids if uid != my_user_id and uid not in l1_set]

    def series_for(user_ids: List[int]) -> Dict[str, int]:
        if not user_ids:
            return {}
        rows = (
            db.query(EarningRecord.stat_date, func.coalesce(func.sum(EarningRecord.coins_total), 0))
            .filter(
                EarningRecord.user_id.in_(user_ids),
                EarningRecord.stat_date.between(start_date, today),
            )
            .group_by(EarningRecord.stat_date)
            .order_by(EarningRecord.stat_date)
            .all()
        )
        return {str(stat_date): int(coins or 0) for stat_date, coins in rows}

    me_series = series_for([my_user_id])
    l1_series = series_for(l1_user_ids)
    l2_series = series_for(l2_user_ids)

    results: List[dict] = []
    for i in range(days):
        d = start_date + timedelta(days=i)
        key = str(d)
        me = int(me_series.get(key, 0))
        l1 = int(l1_series.get(key, 0))
        l2 = int(l2_series.get(key, 0))
        results.append(
            {
                "date": key,
                "me_coins": me,
                "l1_coins": l1,
                "l2_coins": l2,
                "total_coins": me + l1 + l2,
            }
        )
    return results


@router.get("/stats/earnings-trend-by-env")
async def get_earnings_trend_by_env(
    days: int = Query(30, ge=1, le=180),
    end_date: Optional[date] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """近 N 天账号金币趋势（按 env_id，每个账号一条折线），含我 / 一级下级 / 二级下级。"""
    as_of_date = end_date or date.today()
    start_date = as_of_date - timedelta(days=days - 1)

    my_user_id = int(current_user.id)
    descendants = _get_descendant_user_ids(db, my_user_id)
    l1_user_ids = _unique_in_order(descendants["l1"])
    l2_user_ids = _unique_in_order(descendants["l2"])

    l1_user_ids = [uid for uid in l1_user_ids if uid != my_user_id]
    l1_set = set(l1_user_ids)
    l2_user_ids = [uid for uid in l2_user_ids if uid != my_user_id and uid not in l1_set]

    my_env_ids = _get_env_ids_for_users(db, [my_user_id])
    l1_env_ids = _get_env_ids_for_users(db, l1_user_ids)
    l2_env_ids = _get_env_ids_for_users(db, l2_user_ids)
    scope_env_ids = _unique_in_order(my_env_ids + l1_env_ids + l2_env_ids)

    date_keys = [str(start_date + timedelta(days=i)) for i in range(days)]
    if not scope_env_ids:
        return {"dates": date_keys, "accounts": []}

    rows = (
        db.query(
            EarningRecord.env_id,
            EarningRecord.stat_date,
            func.coalesce(func.sum(EarningRecord.coins_total), 0).label("coins_total"),
        )
        .filter(
            EarningRecord.env_id.in_(scope_env_ids),
            EarningRecord.stat_date.between(start_date, as_of_date),
        )
        .group_by(EarningRecord.env_id, EarningRecord.stat_date)
        .all()
    )

    coins_map: Dict[int, Dict[str, int]] = {}
    for env_id, stat_date, coins_total in rows:
        env_id_int = int(env_id)
        coins_map.setdefault(env_id_int, {})[str(stat_date)] = int(coins_total or 0)

    remark_map = _get_env_remark_map(db, scope_env_ids)
    my_env_set = set(my_env_ids)
    l1_env_set = set(l1_env_ids)
    l2_env_set = set(l2_env_ids)

    accounts: List[dict] = []
    for env_id in scope_env_ids:
        series_dict = coins_map.get(int(env_id), {})
        coins_series = [int(series_dict.get(k, 0)) for k in date_keys]
        total_coins = int(sum(coins_series))
        if env_id in my_env_set:
            level = "me"
        elif env_id in l1_env_set:
            level = "l1"
        elif env_id in l2_env_set:
            level = "l2"
        else:
            level = "unknown"
        accounts.append(
            {
                "env_id": int(env_id),
                "remark": (remark_map.get(int(env_id)) or "").strip() or f"账号#{env_id}",
                "level": level,
                "coins": coins_series,
                "total_coins": total_coins,
            }
        )

    return {"dates": date_keys, "accounts": accounts}


@router.get("/stats/earnings-weekly")
async def get_weekly_earnings(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """获取近7天收益趋势（沿用“当前用户可见账号集合”口径）"""
    today = date.today()
    week_ago = today - timedelta(days=6)

    base_query = db.query(EarningRecord)
    if current_user.role != UserRole.ADMIN:
        owned_env_ids = _get_owned_env_ids(db, current_user.id)
        if not owned_env_ids:
            return [{"date": str(week_ago + timedelta(days=i)), "coins_total": 0} for i in range(7)]
        base_query = base_query.filter(EarningRecord.env_id.in_(owned_env_ids))

    results = (
        base_query.filter(EarningRecord.stat_date >= week_ago)
        .with_entities(
            EarningRecord.stat_date,
            func.sum(EarningRecord.coins_total).label("coins_total"),
        )
        .group_by(EarningRecord.stat_date)
        .order_by(EarningRecord.stat_date)
        .all()
    )

    result_dict = {str(r[0]): int(r[1] or 0) for r in results}
    return [
        {"date": str(week_ago + timedelta(days=i)), "coins_total": result_dict.get(str(week_ago + timedelta(days=i)), 0)}
        for i in range(7)
    ]


@router.post("/earnings", response_model=EarningRecordResponse, status_code=status.HTTP_201_CREATED)
async def create_earning(
    data: EarningRecordCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """创建/更新收益记录（通常由系统自动调用）"""
    env = db.query(UserScriptEnv).filter(UserScriptEnv.id == data.env_id).first()
    if not env:
        raise HTTPException(status_code=404, detail="账号不存在（env_id 无效）")

    if current_user.role != UserRole.ADMIN:
        _assert_env_belongs_to_user(db, data.env_id, current_user.id)

    payload = data.model_dump()

    env_remark = (env.remark or "").strip()
    incoming_remark = (payload.get("account_remark") or "").strip()
    if env_remark:
        if incoming_remark and incoming_remark != env_remark:
            raise HTTPException(status_code=400, detail="account_remark 必须与当前账号备注一致")
        account_remark = env_remark
    else:
        account_remark = (incoming_remark or f"账号#{env.id}").strip()
    if not account_remark:
        raise HTTPException(status_code=400, detail="account_remark 不能为空")
    payload["account_remark"] = account_remark

    user_id = env.user_id
    if user_id is None and env.config_id:
        user_id = db.query(UserScriptConfig.user_id).filter(UserScriptConfig.id == env.config_id).scalar()
    payload["user_id"] = int(user_id) if user_id is not None else None

    existing = (
        db.query(EarningRecord)
        .filter(EarningRecord.stat_date == data.stat_date, EarningRecord.account_remark == account_remark)
        .first()
    )

    if existing:
        if int(existing.env_id) != int(data.env_id):
            raise HTTPException(status_code=409, detail="account_remark 与 env_id 不匹配，可能存在备注重复或历史数据异常")
        for key, value in payload.items():
            setattr(existing, key, value)
        db.commit()
        db.refresh(existing)
        return existing

    record = EarningRecord(**payload)
    db.add(record)
    db.commit()
    db.refresh(record)
    return record
