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
    UserScriptEnv,
)
from app.schemas import DashboardStats
from app.auth import get_current_user

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
