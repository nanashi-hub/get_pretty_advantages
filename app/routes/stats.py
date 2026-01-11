from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import date, timedelta
from app.database import get_db
from app.models import (
    User,
    UserScriptConfig,
    QLInstance,
    EarningRecord,
    SettlementPayment,
    SettlementPeriod,
    SettlementUserPayable,
    WalletAccount,
    UserRole,
    UserReferral,
    UserScriptEnv,
    EnvStatus,
)
from app.schemas import DashboardAccountStatusItem, DashboardAccountStatusResponse, DashboardStats
from app.auth import get_current_user
from app.services.account_health import classify_account_health, pick_account_health_basis

router = APIRouter(prefix="/api", tags=["统计"])


@router.get("/stats/dashboard", response_model=DashboardStats)
async def get_dashboard_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """获取仪表板统计数据"""
    today = date.today()
    yesterday = today - timedelta(days=1)
    week_ago = today - timedelta(days=7)

    if current_user.role == UserRole.ADMIN:
        # 管理员看全局数据
        total_users = db.query(User).count()
        total_ks_accounts = db.query(UserScriptEnv).count()
        total_configs = db.query(UserScriptConfig).count()
        total_ql_instances = db.query(QLInstance).count()

        yesterday_coins = db.query(func.sum(EarningRecord.coins_total)).filter(
            EarningRecord.stat_date == yesterday
        ).scalar() or 0

        week_coins = db.query(func.sum(EarningRecord.coins_total)).filter(
            EarningRecord.stat_date >= week_ago
        ).scalar() or 0

        # 管理端：待审核的缴费记录数
        pending_settlements = db.query(SettlementPayment).filter(
            SettlementPayment.status == 0
        ).count()

        # 管理员也显示自己的钱包余额
        wallet = db.query(WalletAccount).filter(WalletAccount.user_id == current_user.id).first()
        available_coins = int(wallet.available_coins or 0) if wallet else 0
        period = (
            db.query(SettlementPeriod)
            .filter(SettlementPeriod.status.in_([0, 1]))
            .order_by(SettlementPeriod.period_id.desc())
            .first()
        )
        coin_rate = int(period.coin_rate) if period and int(getattr(period, "coin_rate", 0) or 0) > 0 else 10000
        wallet_balance = float(available_coins / coin_rate) if coin_rate > 0 else 0.0
    else:
        # 普通用户看自己的数据
        total_users = 0
        total_configs = db.query(UserScriptConfig).filter(
            UserScriptConfig.user_id == current_user.id
        ).count()
        total_ql_instances = db.query(QLInstance).filter(QLInstance.status == 1).count()

        # 当前用户可见账号集合：user_script_configs.user_id -> user_script_envs
        owned_env_ids = [
            env_id
            for (env_id,) in db.query(UserScriptEnv.id)
            .join(UserScriptConfig, UserScriptEnv.config_id == UserScriptConfig.id)
            .filter(UserScriptConfig.user_id == current_user.id)
            .all()
        ]
        total_ks_accounts = len(owned_env_ids)

        if owned_env_ids:
            yesterday_coins = db.query(func.sum(EarningRecord.coins_total)).filter(
                EarningRecord.env_id.in_(owned_env_ids),
                EarningRecord.stat_date == yesterday
            ).scalar() or 0

            week_coins = db.query(func.sum(EarningRecord.coins_total)).filter(
                EarningRecord.env_id.in_(owned_env_ids),
                EarningRecord.stat_date >= week_ago
            ).scalar() or 0
        else:
            yesterday_coins = 0
            week_coins = 0

        # 用户端：当前用户存在未缴清的期数（UNPAID/PARTIAL/OVERDUE）
        pending_settlements = db.query(SettlementUserPayable).filter(
            SettlementUserPayable.user_id == current_user.id,
            SettlementUserPayable.status != 2
        ).count()

        wallet = db.query(WalletAccount).filter(WalletAccount.user_id == current_user.id).first()
        available_coins = int(wallet.available_coins or 0) if wallet else 0
        period = (
            db.query(SettlementPeriod)
            .filter(SettlementPeriod.status.in_([0, 1]))
            .order_by(SettlementPeriod.period_id.desc())
            .first()
        )
        coin_rate = int(period.coin_rate) if period and int(getattr(period, "coin_rate", 0) or 0) > 0 else 10000
        wallet_balance = float(available_coins / coin_rate) if coin_rate > 0 else 0.0

    return DashboardStats(
        total_users=total_users,
        total_ks_accounts=total_ks_accounts,
        total_configs=total_configs,
        total_ql_instances=total_ql_instances,
        yesterday_coins=int(yesterday_coins),
        week_coins=int(week_coins),
        pending_settlements=pending_settlements,
        wallet_balance=wallet_balance
    )


@router.get("/stats/account-health", response_model=DashboardAccountStatusResponse)
async def get_account_health_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """仪表板：自己 + 下级账号状态（今日已统计则用今日，否则用昨日）"""
    stat_date, basis, basis_label = pick_account_health_basis(db)

    # 可见用户集合：自己 + +1/+2 下级（按 user_referrals）
    level_map = {int(current_user.id): ("self", "本人")}
    level1_ids = {
        int(uid)
        for (uid,) in db.query(UserReferral.user_id)
        .filter(UserReferral.inviter_level1 == int(current_user.id))
        .all()
    }
    level2_ids = {
        int(uid)
        for (uid,) in db.query(UserReferral.user_id)
        .filter(UserReferral.inviter_level2 == int(current_user.id))
        .all()
    }
    for uid in level1_ids:
        level_map[uid] = ("l1", "+1")
    for uid in level2_ids:
        level_map.setdefault(uid, ("l2", "+2"))

    user_ids = set(level_map.keys()) | level1_ids | level2_ids
    user_ids_list = list(user_ids)

    # 账号集合：user_script_envs（仅统计 ksck* 变量）
    env_rows = (
        db.query(UserScriptEnv.id, UserScriptEnv.env_name, UserScriptEnv.remark, UserScriptConfig.user_id)
        .join(UserScriptConfig, UserScriptEnv.config_id == UserScriptConfig.id)
        .filter(UserScriptConfig.user_id.in_(user_ids_list))
        .filter(UserScriptEnv.env_name.like("ksck%"))
        .filter(UserScriptEnv.status == EnvStatus.VALID.value)
        .all()
    )

    # 用户展示信息
    users = db.query(User.id, User.username, User.nickname).filter(User.id.in_(user_ids_list)).all()
    user_map = {int(u.id): {"username": u.username, "nickname": u.nickname} for u in users}

    env_ids = [int(env_id) for (env_id, _env_name, _remark, _owner_user_id) in env_rows]
    coins_map = {}
    data_env_ids: set[int] = set()
    if env_ids:
        coin_rows = (
            db.query(EarningRecord.env_id, func.sum(EarningRecord.coins_total).label("coins_total"))
            .filter(EarningRecord.stat_date == stat_date, EarningRecord.env_id.in_(env_ids))
            .group_by(EarningRecord.env_id)
            .all()
        )
        coins_map = {int(env_id): int(total or 0) for (env_id, total) in coin_rows}
        data_env_ids = {int(env_id) for (env_id, _total) in coin_rows}

    counts = {"total": 0, "no_data": 0, "need_config": 0, "black": 0, "edge": 0, "normal": 0}
    items: list[DashboardAccountStatusItem] = []

    for env_id, env_name, remark, owner_user_id in env_rows:
        owner_id = int(owner_user_id) if owner_user_id is not None else None
        relation, relation_label = level_map.get(owner_id, ("other", "其他"))
        owner_info = user_map.get(owner_id) if owner_id is not None else None

        coins = coins_map.get(int(env_id), 0)
        has_data = int(env_id) in data_env_ids
        category, category_label = classify_account_health(has_data, int(coins))

        items.append(
            DashboardAccountStatusItem(
                env_id=int(env_id),
                env_name=str(env_name),
                remark=remark,
                owner_user_id=owner_id,
                owner_username=(owner_info or {}).get("username") if owner_info else None,
                owner_nickname=(owner_info or {}).get("nickname") if owner_info else None,
                relation=relation,
                relation_label=relation_label,
                stat_coins=int(coins),
                category=category,
                category_label=category_label,
            )
        )

        counts["total"] += 1
        counts[category] = counts.get(category, 0) + 1

    severity = {"no_data": 0, "need_config": 1, "black": 2, "edge": 3, "normal": 4}
    items.sort(key=lambda x: (severity.get(x.category, 99), x.stat_coins, x.env_id))

    return DashboardAccountStatusResponse(
        stat_date=stat_date,
        basis=basis,
        basis_label=basis_label,
        counts=counts,
        items=items,
    )
