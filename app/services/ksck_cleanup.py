import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import case, func
from sqlalchemy.orm import Session

from app.models import EarningRecord, EnvStatus, QLInstance, UserScriptConfig, UserScriptEnv
from app.services.qinglong import QingLongClient

logger = logging.getLogger(__name__)

ARCHIVE_PREFIX = "__archived__"


@dataclass(frozen=True)
class KsckCleanupResult:
    days: int
    stat_end_date: Optional[date]
    stat_start_date: Optional[date]
    candidates: int
    archived: int
    ql_deleted: int
    ql_delete_failed: int


def build_archived_env_name(old_env_name: str, env_id: int) -> str:
    """生成归档后的 env_name（确保不再以 ksck 开头，从而释放序号）"""
    old = (old_env_name or "").strip()
    suffix = f"_{env_id}"
    base = f"{ARCHIVE_PREFIX}{old}{suffix}"
    if len(base) <= 100:
        return base
    remaining = 100 - len(ARCHIVE_PREFIX) - len(suffix)
    if remaining <= 0:
        return f"{ARCHIVE_PREFIX}{env_id}"
    return f"{ARCHIVE_PREFIX}{old[:remaining]}{suffix}"


def _get_qinglong_client(db: Session, config: UserScriptConfig) -> Optional[QingLongClient]:
    if not config.ql_instance_id:
        return None
    instance = (
        db.query(QLInstance)
        .filter(QLInstance.id == config.ql_instance_id, QLInstance.status == 1)
        .first()
    )
    if not instance:
        return None
    return QingLongClient(instance)


def _latest_earning_date(db: Session) -> Optional[date]:
    return db.query(func.max(EarningRecord.stat_date)).scalar()


def find_need_config_streak_env_ids(
    db: Session,
    days: int,
    *,
    end_date: Optional[date] = None,
) -> tuple[Optional[date], Optional[date], list[int]]:
    """
    查找在最近连续 days 天内，每天都有统计记录且金币<=0 的 env_id 列表。
    - 不把“无记录”当成 0：无记录会打断连续性，避免误删未跑脚本的账号。
    """
    if days <= 0:
        return None, None, []

    stat_end = end_date or _latest_earning_date(db)
    if not stat_end:
        return None, None, []

    stat_start = stat_end - timedelta(days=days - 1)

    day_sums_sq = (
        db.query(
            EarningRecord.env_id.label("env_id"),
            EarningRecord.stat_date.label("stat_date"),
            func.sum(EarningRecord.coins_total).label("coins_total"),
        )
        .filter(EarningRecord.stat_date >= stat_start, EarningRecord.stat_date <= stat_end)
        .group_by(EarningRecord.env_id, EarningRecord.stat_date)
        .subquery()
    )

    need_days = func.sum(case((day_sums_sq.c.coins_total <= 0, 1), else_=0))
    total_days = func.count(day_sums_sq.c.stat_date)

    env_ids = [
        int(env_id)
        for (env_id,) in (
            db.query(day_sums_sq.c.env_id)
            .group_by(day_sums_sq.c.env_id)
            .having(total_days == days)
            .having(need_days == days)
            .all()
        )
    ]
    return stat_start, stat_end, env_ids


def archive_need_config_streak_envs(
    db: Session,
    *,
    days: int = 15,
    dry_run: bool = False,
    delete_in_qinglong: bool = True,
) -> KsckCleanupResult:
    """
    归档连续 N 天“需更换配置”的 ksck 账号：
    - 物理删除 user_script_envs 会破坏收益/结算链路（earning_records 依赖 env_id），因此采用“归档”：
      1) best-effort 删除青龙变量（如 ql_env_id 存在）
      2) 本地改名为 __archived__ 前缀（释放 ksck 序号），清空 CK，状态置为 invalid，解绑 IP
      3) 从配置环境列表中默认隐藏（由 list 接口过滤 __archived__ 前缀）
    """
    stat_start, stat_end, env_ids = find_need_config_streak_env_ids(db, days)
    if not env_ids:
        return KsckCleanupResult(
            days=days,
            stat_end_date=stat_end,
            stat_start_date=stat_start,
            candidates=0,
            archived=0,
            ql_deleted=0,
            ql_delete_failed=0,
        )

    envs = (
        db.query(UserScriptEnv)
        .filter(
            UserScriptEnv.id.in_(env_ids),
            UserScriptEnv.env_name.like("ksck%"),
            UserScriptEnv.status == EnvStatus.VALID.value,
        )
        .all()
    )

    if dry_run:
        return KsckCleanupResult(
            days=days,
            stat_end_date=stat_end,
            stat_start_date=stat_start,
            candidates=len(envs),
            archived=0,
            ql_deleted=0,
            ql_delete_failed=0,
        )

    ip_ids_to_recalc: set[int] = set()
    user_ip_ids_to_recalc: set[int] = set()
    archived = 0
    ql_deleted = 0
    ql_delete_failed = 0

    configs = {
        int(c.id): c
        for c in db.query(UserScriptConfig).filter(UserScriptConfig.id.in_({e.config_id for e in envs})).all()
    }

    for env in envs:
        old_ip_id = int(env.ip_id) if env.ip_id else None
        old_user_ip_id = int(env.user_ip_id) if env.user_ip_id else None

        config = configs.get(int(env.config_id))

        if delete_in_qinglong and env.ql_env_id and config:
            client = _get_qinglong_client(db, config)
            if client:
                try:
                    client.delete_env(env.ql_env_id)
                    ql_deleted += 1
                except Exception as exc:
                    ql_delete_failed += 1
                    logger.warning(
                        "删除青龙变量失败 env_id=%s ql_env_id=%s: %s",
                        env.id,
                        env.ql_env_id,
                        exc,
                    )

        env.env_name = build_archived_env_name(env.env_name, int(env.id))
        env.env_value = ""
        env.ql_env_id = None
        env.status = EnvStatus.INVALID.value

        env.ip_id = None
        env.user_ip_id = None
        env.disabled_until = None
        env.disable_days = None
        env.disabled_at = None

        if old_ip_id:
            ip_ids_to_recalc.add(old_ip_id)
        if old_user_ip_id:
            user_ip_ids_to_recalc.add(old_user_ip_id)

        archived += 1

    db.flush()

    if ip_ids_to_recalc:
        from app.routes.config_envs import recalc_ip_usage

        recalc_ip_usage(db, ip_ids_to_recalc)
    if user_ip_ids_to_recalc:
        from app.routes.config_envs import recalc_user_ip_usage

        recalc_user_ip_usage(db, user_ip_ids_to_recalc)

    db.commit()

    return KsckCleanupResult(
        days=days,
        stat_end_date=stat_end,
        stat_start_date=stat_start,
        candidates=len(envs),
        archived=archived,
        ql_deleted=ql_deleted,
        ql_delete_failed=ql_delete_failed,
    )

