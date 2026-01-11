import random
import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Set
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.auth import get_current_user
from app.database import get_db
from app.logging_config import get_logger
from app.models import (
    EnvStatus,
    ConfigStatus,
    IPPool,
    UserIPPool,
    QLInstance,
    User,
    UserReferral,
    UserRole,
    UserScriptConfig,
    UserScriptEnv,
    EarningRecord,
)
from app.schemas import (
    UserScriptConfigResponse,
    UserScriptEnvResponse,
)
from app.services.account_health import classify_account_health, pick_account_health_basis
from app.services.qinglong import QingLongClient

router = APIRouter(prefix="/api/config-envs", tags=["配置环境"])

DEFAULT_QL_NAME = "默认青龙实例"
DEFAULT_QL_BASE_URL = "http://192.168.5.204:1116"
DEFAULT_QL_CLIENT_ID = "N16sNCmXwY_S"
DEFAULT_QL_CLIENT_SECRET = "rr_tBarvo4lwvDnbzKyJhq2j"
DEFAULT_QL_REMARK = "自动创建的默认青龙实例（来自配置环境模块）"

IP_MODE_SYSTEM_RANDOM = "system_random"
IP_MODE_USER_POOL = "user_pool"
VALID_IP_MODES = {IP_MODE_SYSTEM_RANDOM, IP_MODE_USER_POOL}
VALID_IP_STATUSES = {"active", "disabled"}


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """要求管理员权限"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="仅管理员可操作")
    return current_user


class KSCKEnvPayload(BaseModel):
    """新增/修改 ksck 变量的载荷"""
    cookie: Optional[str] = Field(None, description="ksck 值（必填）")
    remark: Optional[str] = Field(None, description="备注")
    ip_mode: Optional[str] = Field(None, description="IP模式：system_random/user_pool")
    ip_id: Optional[int] = Field(None, description="IP池ID")
    user_ip_id: Optional[int] = Field(None, description="用户自有代理池ID")
    status: Optional[str] = Field(None, description="valid/invalid")


def ensure_default_ql_instance(db: Session) -> QLInstance:
    """如果不存在默认青龙实例则自动创建"""
    instance = (
        db.query(QLInstance)
        .filter(QLInstance.base_url == DEFAULT_QL_BASE_URL)
        .first()
    )
    if instance:
        return instance

    instance = (
        db.query(QLInstance)
        .filter(QLInstance.name == DEFAULT_QL_NAME)
        .first()
    )
    if instance:
        return instance

    instance = QLInstance(
        name=DEFAULT_QL_NAME,
        base_url=DEFAULT_QL_BASE_URL,
        client_id=DEFAULT_QL_CLIENT_ID,
        client_secret=DEFAULT_QL_CLIENT_SECRET,
        remark=DEFAULT_QL_REMARK,
        status=1,
    )
    db.add(instance)
    db.commit()
    db.refresh(instance)
    return instance


def build_proxy_url(ip: Optional[IPPool]) -> str:
    """构造代理URL字符串"""
    if not ip:
        return ""
    if ip.proxy_url:
        return ip.proxy_url
    auth = ""
    if ip.username and ip.password:
        auth = f"{ip.username}:{ip.password}@"
    elif ip.username:
        auth = f"{ip.username}@"
    return f"{auth}{ip.ip}:{ip.port}"


def build_user_proxy_url(ip: Optional[UserIPPool]) -> str:
    """构造用户自有代理URL（强制 socks5://）"""
    if not ip:
        return ""
    if ip.proxy_url:
        return ip.proxy_url
    username = (ip.username or "").strip()
    password = (ip.password or "").strip()
    if not username or not password:
        raise HTTPException(status_code=400, detail="自有代理缺少账号或密码，无法拼接 proxy_url")
    return f"socks5://{username}:{password}@{ip.ip}:{ip.port}"


def build_ql_value(env: UserScriptEnv, proxy_url: str) -> str:
    """按 备注#cookie#proxy_url 组合青龙变量值"""
    remark = env.remark or ""
    cookie = env.env_value or ""
    return f"{remark}#{cookie}#{proxy_url or ''}"


def recalc_ip_usage(db: Session, ip_ids: Optional[Set[int]] = None) -> None:
    """刷新 IP 使用次数到 ip_pool.usage_count（不使用触发器）"""
    # 统计当前使用数
    usage_query = db.query(UserScriptEnv.ip_id, func.count(UserScriptEnv.id)).filter(
        UserScriptEnv.ip_id.isnot(None),
        UserScriptEnv.status == EnvStatus.VALID.value,
    )
    if ip_ids:
        usage_query = usage_query.filter(UserScriptEnv.ip_id.in_(ip_ids))
    usage_rows = usage_query.group_by(UserScriptEnv.ip_id).all()
    usage_map = {ip_id: count for ip_id, count in usage_rows}

    targets = (
        set(ip_id for (ip_id,) in db.query(IPPool.id).all())
        if ip_ids is None
        else set(ip_ids)
    )
    if not targets:
        return
    for ip_id in targets:
        db.query(IPPool).filter(IPPool.id == ip_id).update(
            {"usage_count": usage_map.get(ip_id, 0)}
        )
    db.flush()


def recalc_user_ip_usage(db: Session, user_ip_ids: Optional[Set[int]] = None) -> None:
    """刷新用户自有 IP 使用次数到 user_ip_pool.usage_count（不使用触发器）"""
    usage_query = db.query(UserScriptEnv.user_ip_id, func.count(UserScriptEnv.id)).filter(
        UserScriptEnv.user_ip_id.isnot(None),
        UserScriptEnv.status == EnvStatus.VALID.value,
    )
    if user_ip_ids:
        usage_query = usage_query.filter(UserScriptEnv.user_ip_id.in_(user_ip_ids))
    usage_rows = usage_query.group_by(UserScriptEnv.user_ip_id).all()
    usage_map = {ip_id: count for ip_id, count in usage_rows}

    targets = (
        set(ip_id for (ip_id,) in db.query(UserIPPool.id).all())
        if user_ip_ids is None
        else set(user_ip_ids)
    )
    if not targets:
        return
    for ip_id in targets:
        db.query(UserIPPool).filter(UserIPPool.id == ip_id).update(
            {"usage_count": usage_map.get(ip_id, 0)}
        )
    db.flush()


def normalize_ip_mode_or_default(ip_mode: Optional[str]) -> str:
    """规范化 IP 模式（缺省为 system_random）"""
    mode = (ip_mode or IP_MODE_SYSTEM_RANDOM).strip()
    if mode not in VALID_IP_MODES:
        raise HTTPException(status_code=400, detail="IP 模式无效，仅支持 system_random/user_pool")
    return mode


def can_manage_user(current_user: User, target_user_id: int, db: Session) -> bool:
    """判断是否有权限管理目标用户"""
    if current_user.role == UserRole.ADMIN:
        return True
    if current_user.id == target_user_id:
        return True

    referral = (
        db.query(UserReferral).filter(UserReferral.user_id == target_user_id).first()
    )
    if not referral:
        return False

    if referral.inviter_level1 == current_user.id:
        return True
    if referral.inviter_level2 == current_user.id:
        return True
    return False


def can_create_env(current_user: User, target_user_id: int, db: Session) -> bool:
    """是否允许为目标用户新增环境变量"""
    if current_user.role == UserRole.ADMIN:
        return True
    return target_user_id in get_manageable_user_ids(current_user, db)


def assert_config_permission(
    current_user: User, config: UserScriptConfig, db: Session
) -> None:
    """校验当前用户是否可操作配置"""
    if not can_manage_user(current_user, config.user_id, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="无权操作此配置"
        )


def get_manageable_user_ids(current_user: User, db: Session) -> Set[int]:
    """
    获取当前用户可管理的用户ID集合
    - 管理员：全部
    - 普通用户：永远包含自己；如果有下级（作为 inviter_level1 / inviter_level2），则再加上下级
    """
    if current_user.role == UserRole.ADMIN:
        return set(uid for (uid,) in db.query(User.id).filter(User.status == 1).all())

    level1_ids = {
        uid
        for (uid,) in db.query(UserReferral.user_id).filter(
            UserReferral.inviter_level1 == current_user.id
        ).all()
    }
    level2_ids = {
        uid
        for (uid,) in db.query(UserReferral.user_id).filter(
            UserReferral.inviter_level2 == current_user.id
        ).all()
    }
    downstream = level1_ids | level2_ids
    return {current_user.id} | downstream


def get_manageable_users(current_user: User, db: Session):
    """获取可管理的用户信息列表"""
    ids = get_manageable_user_ids(current_user, db)
    users = (
        db.query(User)
        .filter(User.id.in_(ids), User.status == 1)
        .order_by(User.id)
        .all()
    )
    return [
        {
            "id": u.id,
            "username": u.username,
            "nickname": u.nickname,
            "role": u.role.value if hasattr(u.role, "value") else u.role,
        }
        for u in users
    ]


def generate_env_name(db: Session, config_id: int) -> str:
    """生成全局顺序变量名 ksck1..ksck888（忽略其他前缀，复用缺口）"""
    existing = [
        name
        for (name,) in db.query(UserScriptEnv.env_name).filter(
            UserScriptEnv.env_name.like("ksck%")
        )
    ]
    prefix = "ksck"
    used = set()
    for name in existing:
        if not name.startswith(prefix):
            continue
        suffix = name[len(prefix):]
        if suffix == "":
            used.add(1)
        elif suffix.isdigit():
            used.add(int(suffix))
    for i in range(1, 889):  # 1..888
        if i not in used:
            return f"{prefix}{i}"
    raise HTTPException(status_code=400, detail="ksck 序号已用尽（1-888）")


@router.get("/next-name")
async def get_next_env_name(
    db: Session = Depends(get_db), current_user: User = Depends(get_current_user)
):
    """查询下一个可用 ksck 序号（全局 1-888，复用缺口）"""
    name = generate_env_name(db, config_id=0)
    return {"next_name": name}


def get_ip_with_usage(
    db: Session, ip_id: int, exclude_env_id: Optional[int] = None
) -> IPPool:
    """校验IP可用性并返回IP"""
    ip = (
        db.query(IPPool)
        .filter(IPPool.id == ip_id, IPPool.status == "active")
        .first()
    )
    if not ip:
        raise HTTPException(status_code=404, detail="IP不存在或已禁用")
    if ip.expire_date and ip.expire_date < date.today():
        raise HTTPException(status_code=400, detail="IP已过期")

    usage_query = db.query(func.count(UserScriptEnv.id)).filter(
        UserScriptEnv.ip_id == ip_id,
        UserScriptEnv.status == EnvStatus.VALID.value,
    )
    if exclude_env_id:
        usage_query = usage_query.filter(UserScriptEnv.id != exclude_env_id)
    used = usage_query.scalar() or 0
    if used >= ip.max_users:
        raise HTTPException(status_code=400, detail="该IP使用已达上限")
    return ip


def pick_random_system_ip(db: Session, exclude_env_id: Optional[int] = None) -> IPPool:
    """从系统 IP 池中随机挑选一个可用 IP（容量/过期/状态校验）"""
    ips = (
        db.query(IPPool)
        .filter(
            IPPool.status == "active",
            (IPPool.expire_date.is_(None)) | (IPPool.expire_date >= date.today()),
        )
        .all()
    )
    if not ips:
        raise HTTPException(status_code=400, detail="系统 IP 池为空或无可用 IP")

    ip_ids = [ip.id for ip in ips]
    usage_query = db.query(UserScriptEnv.ip_id, func.count(UserScriptEnv.id)).filter(
        UserScriptEnv.ip_id.in_(ip_ids),
        UserScriptEnv.status == EnvStatus.VALID.value,
    )
    if exclude_env_id:
        usage_query = usage_query.filter(UserScriptEnv.id != exclude_env_id)
    usage_rows = usage_query.group_by(UserScriptEnv.ip_id).all()
    usage_map = {ip_id: count for ip_id, count in usage_rows}

    candidates = [
        ip
        for ip in ips
        if (usage_map.get(ip.id, 0) < (ip.max_users or 2))
    ]
    if not candidates:
        raise HTTPException(status_code=400, detail="系统 IP 池暂无可用 IP（容量已满）")
    return random.choice(candidates)


def get_user_ip_with_usage(
    db: Session,
    user_id: int,
    user_ip_id: int,
    exclude_env_id: Optional[int] = None,
) -> UserIPPool:
    """校验用户自有 IP 可用性并返回（归属/过期/容量）"""
    ip = (
        db.query(UserIPPool)
        .filter(
            UserIPPool.id == user_ip_id,
            UserIPPool.user_id == user_id,
            UserIPPool.status == "active",
        )
        .first()
    )
    if not ip:
        raise HTTPException(status_code=404, detail="自有代理不存在或已禁用")
    if ip.expire_date and ip.expire_date < date.today():
        raise HTTPException(status_code=400, detail="自有代理已过期")

    usage_query = db.query(func.count(UserScriptEnv.id)).filter(
        UserScriptEnv.user_ip_id == user_ip_id,
        UserScriptEnv.status == EnvStatus.VALID.value,
    )
    if exclude_env_id:
        usage_query = usage_query.filter(UserScriptEnv.id != exclude_env_id)
    used = usage_query.scalar() or 0
    if used >= (ip.max_users or 2):
        raise HTTPException(status_code=400, detail="该自有代理使用已达上限")
    return ip

def normalize_remark_or_400(remark: Optional[str]) -> str:
    """备注去空格并强制必填"""
    normalized = (remark or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="备注不能为空（用于收益统计唯一标识）")
    return normalized


def normalize_cookie_or_400(cookie: Optional[str]) -> str:
    """ksck 去空格并强制必填"""
    normalized = (cookie or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="请填写 ksck 值")
    return normalized


def assert_unique_remark(db: Session, remark: str, exclude_env_id: Optional[int] = None) -> None:
    """校验备注唯一（全表唯一）"""
    query = db.query(UserScriptEnv.id).filter(UserScriptEnv.remark == remark)
    if exclude_env_id is not None:
        query = query.filter(UserScriptEnv.id != exclude_env_id)
    exists = db.query(query.exists()).scalar()
    if exists:
        raise HTTPException(status_code=400, detail="备注必须为唯一值")


def get_config_or_404(config_id: int, db: Session) -> UserScriptConfig:
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    return config


def get_env_or_404(env_id: int, config_id: int, db: Session) -> UserScriptEnv:
    env = (
        db.query(UserScriptEnv)
        .options(joinedload(UserScriptEnv.ip), joinedload(UserScriptEnv.user_ip))  # 预加载 IP 关联
        .filter(
            UserScriptEnv.id == env_id,
            UserScriptEnv.config_id == config_id,
        )
        .first()
    )
    if not env:
        raise HTTPException(status_code=404, detail="环境变量不存在")
    return env


def get_ql_client_for_config(config: UserScriptConfig, db: Session) -> QingLongClient:
    """获取配置对应的青龙客户端，若未配置则自动绑定默认实例"""
    instance = (
        db.query(QLInstance)
        .filter(QLInstance.id == config.ql_instance_id)
        .first()
        if config.ql_instance_id
        else None
    )
    if not instance:
        instance = ensure_default_ql_instance(db)
        config.ql_instance_id = instance.id
        db.commit()
        db.refresh(config)

    if instance.status != 1:
        raise HTTPException(status_code=400, detail="青龙实例已停用")
    return QingLongClient(instance)


def sync_env_to_ql(
    client: QingLongClient,
    env: UserScriptEnv,
    config_id: int,
    enable: Optional[bool],
    proxy_url: str = "",
) -> str:
    """同步本地环境变量到青龙并返回青龙ID"""
    ql_value = build_ql_value(env, proxy_url)
    remarks = (env.remark or f"配置ID:{config_id}")[:200]
    result = client.sync_env(
        name=env.env_name,
        value=ql_value,
        remarks=remarks,
        enabled=enable if enable is not None else env.status == EnvStatus.VALID.value,
    )
    ql_env_id = result.get("id") or result.get("_id")
    if not ql_env_id:
        raise RuntimeError("未能获取青龙环境变量ID")
    return str(ql_env_id)


@router.get("/configs", response_model=List[UserScriptConfigResponse])
async def list_manageable_configs(
    db: Session = Depends(get_db), current_user: User = Depends(get_current_user)
):
    """列出当前用户可管理的配置列表"""
    manageable_ids = get_manageable_user_ids(current_user, db)
    query = db.query(UserScriptConfig).filter(
        UserScriptConfig.user_id.in_(manageable_ids)
    )
    configs = query.order_by(UserScriptConfig.id.desc()).all()
    return configs


@router.get("/managed-users")
async def list_managed_users(
    db: Session = Depends(get_db), current_user: User = Depends(get_current_user)
):
    """列出当前用户可管理的用户（用于选择分配对象）"""
    return {"data": get_manageable_users(current_user, db)}


@router.get("/managed-envs")
async def list_managed_envs(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    批量获取当前用户可管理范围内的环境变量列表（用于配置环境页面快速加载）
    - 默认仅取每个用户的「默认配置」（即该用户最早创建的 user_script_configs.id）
    - IP 占用数按全局 status='valid' 口径统计（不写入 usage_count）
    """
    manageable_ids = get_manageable_user_ids(current_user, db)
    if not manageable_ids:
        return {"data": [], "total": 0}

    # 每个用户取最早创建的配置作为“默认配置”（与前端旧逻辑保持一致）
    default_config_sq = (
        db.query(
            UserScriptConfig.user_id.label("user_id"),
            func.min(UserScriptConfig.id).label("config_id"),
        )
        .filter(UserScriptConfig.user_id.in_(manageable_ids))
        .group_by(UserScriptConfig.user_id)
        .subquery()
    )

    rows = (
        db.query(
            UserScriptEnv.id,
            UserScriptEnv.config_id,
            UserScriptEnv.env_name,
            UserScriptEnv.env_value,
            UserScriptEnv.ql_env_id,
            UserScriptEnv.ip_mode,
            UserScriptEnv.ip_id,
            UserScriptEnv.user_ip_id,
            UserScriptEnv.status,
            UserScriptEnv.remark,
            UserScriptEnv.disabled_until,
            UserScriptEnv.created_at,
            UserScriptEnv.updated_at,
            User.id.label("user_id"),
            User.username.label("user_name"),
            User.nickname.label("user_nickname"),
            User.role.label("user_role"),
        )
        .join(default_config_sq, UserScriptEnv.config_id == default_config_sq.c.config_id)
        .join(User, User.id == default_config_sq.c.user_id)
        .filter(~UserScriptEnv.env_name.like("__archived__%"))
        .order_by(UserScriptEnv.id.desc())
        .all()
    )

    # 账号状态提醒：与仪表板口径一致（今日有数据用今日，否则用昨日）
    stat_date, basis, basis_label = pick_account_health_basis(db)
    ks_env_ids_set = {
        int(r.id)
        for r in rows
        if str(getattr(r, "env_name", "") or "").lower().startswith("ksck")
        and getattr(r, "status", None) == EnvStatus.VALID.value
    }
    coins_map = {}
    data_env_ids: set[int] = set()
    if ks_env_ids_set:
        coin_rows = (
            db.query(EarningRecord.env_id, func.sum(EarningRecord.coins_total).label("coins_total"))
            .filter(
                EarningRecord.stat_date == stat_date,
                EarningRecord.env_id.in_(list(ks_env_ids_set)),
            )
            .group_by(EarningRecord.env_id)
            .all()
        )
        coins_map = {int(env_id): int(total or 0) for (env_id, total) in coin_rows}
        data_env_ids = {int(env_id) for (env_id, _total) in coin_rows}

    system_ip_ids = {r.ip_id for r in rows if r.ip_id}
    user_ip_ids = {r.user_ip_id for r in rows if r.user_ip_id}

    system_ip_map = (
        {ip.id: ip for ip in db.query(IPPool).filter(IPPool.id.in_(system_ip_ids)).all()}
        if system_ip_ids
        else {}
    )
    user_ip_map = (
        {
            ip.id: ip
            for ip in db.query(UserIPPool).filter(UserIPPool.id.in_(user_ip_ids)).all()
        }
        if user_ip_ids
        else {}
    )

    system_usage_map = {}
    if system_ip_ids:
        usage_rows = (
            db.query(UserScriptEnv.ip_id, func.count(UserScriptEnv.id))
            .filter(
                UserScriptEnv.ip_id.in_(system_ip_ids),
                UserScriptEnv.status == EnvStatus.VALID.value,
            )
            .group_by(UserScriptEnv.ip_id)
            .all()
        )
        system_usage_map = {ip_id: int(count or 0) for ip_id, count in usage_rows}

    user_usage_map = {}
    if user_ip_ids:
        usage_rows = (
            db.query(UserScriptEnv.user_ip_id, func.count(UserScriptEnv.id))
            .filter(
                UserScriptEnv.user_ip_id.in_(user_ip_ids),
                UserScriptEnv.status == EnvStatus.VALID.value,
            )
            .group_by(UserScriptEnv.user_ip_id)
            .all()
        )
        user_usage_map = {ip_id: int(count or 0) for ip_id, count in usage_rows}

    data = []
    for r in rows:
        mode = (r.ip_mode or IP_MODE_SYSTEM_RANDOM).strip()
        if mode not in VALID_IP_MODES:
            mode = IP_MODE_SYSTEM_RANDOM

        ip_info = None
        user_ip_info = None
        account_health = None

        if mode == IP_MODE_USER_POOL and r.user_ip_id:
            ip_obj = user_ip_map.get(r.user_ip_id)
            if ip_obj:
                used = int(user_usage_map.get(ip_obj.id, 0))
                user_ip_info = {
                    "id": ip_obj.id,
                    "proxy_url": build_user_proxy_url(ip_obj),
                    "region": ip_obj.region,
                    "vendor": ip_obj.vendor,
                    "max_users": ip_obj.max_users or 2,
                    "used": used,
                }
        elif r.ip_id:
            ip_obj = system_ip_map.get(r.ip_id)
            if ip_obj:
                used = int(system_usage_map.get(ip_obj.id, 0))
                ip_info = {
                    "id": ip_obj.id,
                    "proxy_url": build_proxy_url(ip_obj),
                    "region": ip_obj.region,
                    "vendor": ip_obj.vendor,
                    "max_users": ip_obj.max_users or 2,
                    "used": used,
                }

        env_name_lower = str(getattr(r, "env_name", "") or "").lower()
        if env_name_lower.startswith("ksck"):
            if r.status != EnvStatus.VALID.value:
                account_health = {
                    "stat_coins": 0,
                    "category": "disabled",
                    "category_label": "已禁用",
                    "has_data": False,
                }
            else:
                coins = int(coins_map.get(int(r.id), 0))
                has_data = int(r.id) in data_env_ids
                category, category_label = classify_account_health(has_data, coins)
                account_health = {
                    "stat_coins": coins,
                    "category": category,
                    "category_label": category_label,
                    "has_data": bool(has_data),
                }

        data.append(
            {
                "id": r.id,
                "config_id": r.config_id,
                "env_name": r.env_name,
                "env_value": r.env_value,
                "ql_env_id": r.ql_env_id,
                "ip_mode": mode,
                "ip_id": r.ip_id,
                "ip_info": ip_info,
                "user_ip_id": r.user_ip_id,
                "user_ip_info": user_ip_info,
                "account_health": account_health,
                "status": r.status,
                "remark": r.remark,
                "disabled_until": r.disabled_until.isoformat() if r.disabled_until else None,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
                "user_id": r.user_id,
                "user_name": r.user_name,
                "user_nickname": r.user_nickname,
                "user_role": r.user_role,
            }
        )

    return {
        "data": data,
        "total": len(data),
        "account_health_basis": {
            "stat_date": stat_date.isoformat(),
            "basis": basis,
            "basis_label": basis_label,
        },
    }


@router.post("/users/{user_id}/default-config")
async def ensure_default_config(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """获取或创建某用户的默认配置（供配置环境使用）"""
    if not can_manage_user(current_user, user_id, db):
        raise HTTPException(status_code=403, detail="无权管理此用户")

    config = (
        db.query(UserScriptConfig)
        .filter(UserScriptConfig.user_id == user_id)
        .order_by(UserScriptConfig.id.asc())
        .first()
    )
    if not config:
        default_ql = ensure_default_ql_instance(db)
        config = UserScriptConfig(
            user_id=user_id,
            ql_instance_id=default_ql.id,
            script_name="default",
            group_key=f"default_{user_id}",
            status=getattr(ConfigStatus, "ENABLED", "enabled"),
        )
        db.add(config)
        db.commit()
        db.refresh(config)
    return {"config_id": config.id}


@router.get("/ip-pool/available")
async def list_available_ips(
    db: Session = Depends(get_db), current_user: User = Depends(get_current_user)
):
    """获取IP池列表（包含容量信息）"""
    ips = (
        db.query(IPPool)
        .filter(
            IPPool.status == "active",
            (IPPool.expire_date.is_(None)) | (IPPool.expire_date >= date.today()),
        )
        .order_by(IPPool.id.desc())
        .all()
    )

    ip_ids = [ip.id for ip in ips]
    usage_map = {}
    if ip_ids:
        usage_rows = (
            db.query(UserScriptEnv.ip_id, func.count(UserScriptEnv.id))
            .filter(
                UserScriptEnv.ip_id.in_(ip_ids),
                UserScriptEnv.status == EnvStatus.VALID.value,
            )
            .group_by(UserScriptEnv.ip_id)
            .all()
        )
        usage_map = {ip_id: int(count or 0) for ip_id, count in usage_rows}

    available = []
    for ip in ips:
        used = usage_map.get(ip.id, 0)
        available.append(
            {
                "id": ip.id,
                "proxy_url": build_proxy_url(ip),
                "region": ip.region,
                "vendor": ip.vendor,
                "max_users": ip.max_users or 2,
                "used": used,
                "usage_count": used,
            }
        )
    return {"data": available}


class IPPoolCreatePayload(BaseModel):
    """新增系统代理 IP"""
    ip: str = Field(..., description="IP")
    port: int = Field(..., ge=1, le=65535, description="端口")
    username: Optional[str] = Field(None, description="代理账号（可选）")
    password: Optional[str] = Field(None, description="代理密码（可选）")
    proxy_url: Optional[str] = Field(None, description="完整代理URL（可选，优先使用）")
    region: Optional[str] = Field(None, description="地区/城市")
    vendor: Optional[str] = Field(None, description="供应商")
    expire_date: Optional[date] = Field(None, description="到期时间")
    max_users: Optional[int] = Field(None, ge=1, le=20, description="最多同时使用人数（默认2）")
    status: Optional[str] = Field(None, description="active/disabled（默认active）")


class IPPoolUpdatePayload(BaseModel):
    """更新系统代理 IP（支持续期）"""
    ip: Optional[str] = Field(None, description="IP")
    port: Optional[int] = Field(None, ge=1, le=65535, description="端口")
    username: Optional[str] = Field(None, description="代理账号（可选）")
    password: Optional[str] = Field(None, description="代理密码（可选）")
    proxy_url: Optional[str] = Field(None, description="完整代理URL（可选）")
    region: Optional[str] = Field(None, description="地区/城市")
    vendor: Optional[str] = Field(None, description="供应商")
    expire_date: Optional[date] = Field(None, description="到期时间（续期）")
    max_users: Optional[int] = Field(None, ge=1, le=20, description="最多同时使用人数")
    status: Optional[str] = Field(None, description="active/disabled")


class IPPoolImportPayload(BaseModel):
    """批量导入系统代理 IP（支持覆盖更新）"""
    text: str = Field(..., description="每行一条记录")
    default_expire_date: Optional[date] = Field(None, description="默认到期时间（留空则不覆盖）")
    default_vendor: Optional[str] = Field(None, description="默认供应商（留空则不覆盖）")
    default_region: Optional[str] = Field(None, description="默认地区（留空则不覆盖）")
    default_max_users: Optional[int] = Field(None, ge=1, le=20, description="默认最大使用人数（留空则不覆盖）")
    default_status: Optional[str] = Field(None, description="默认状态 active/disabled（留空则不覆盖）")
    overwrite: bool = Field(True, description="已存在（ip+port）时是否覆盖更新")


class IPPoolBulkIdsPayload(BaseModel):
    """批量操作：ID 列表"""
    ids: List[int] = Field(..., min_length=1, description="IP 记录ID列表")


class IPPoolBulkStatusPayload(IPPoolBulkIdsPayload):
    """批量操作：启用/禁用"""
    status: str = Field(..., description="active/disabled")


class IPPoolBulkExtendPayload(IPPoolBulkIdsPayload):
    """批量操作：续期"""
    days: int = Field(..., ge=1, le=3650, description="续期天数（如 30/60/90）")
    from_today_if_expired: bool = Field(True, description="已过期时从今天开始续期（否则从原到期日续）")


def _normalize_ip_status_or_400(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = (value or "").strip().lower()
    if normalized not in VALID_IP_STATUSES:
        raise HTTPException(status_code=400, detail="IP 状态无效，仅支持 active/disabled")
    return normalized


_DATE_RE = re.compile(r"^\\d{4}-\\d{2}-\\d{2}$")


def _parse_date_or_none(raw: str) -> Optional[date]:
    if not raw:
        return None
    token = raw.strip()
    if not token:
        return None
    if not _DATE_RE.match(token):
        return None
    return datetime.strptime(token, "%Y-%m-%d").date()


def _parse_system_ip_base_or_400(base: str) -> dict:
    """
    解析导入行的第一个token（基础字段）
    支持：
    - scheme://username:password@ip:port
    - username:password@ip:port
    - ip:port
    - ip:port:username:password
    """
    token = (base or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="IP 记录为空")

    if "://" in token:
        parsed = urlparse(token)
        if not parsed.hostname or not parsed.port:
            raise HTTPException(status_code=400, detail=f"代理URL解析失败: {token}")
        return {
            "ip": parsed.hostname,
            "port": int(parsed.port),
            "username": parsed.username,
            "password": parsed.password,
            "proxy_url": token,
        }

    if "@" in token:
        if token.count("@") != 1:
            raise HTTPException(status_code=400, detail=f"代理格式不合法: {token}")
        auth_part, host_part = token.split("@", 1)
        if ":" not in host_part:
            raise HTTPException(status_code=400, detail=f"代理格式不合法: {token}")
        host, port_str = host_part.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"端口解析失败: {token}") from exc
        username = None
        password = None
        if auth_part:
            if ":" in auth_part:
                username, password = auth_part.split(":", 1)
            else:
                username = auth_part
        return {
            "ip": host.strip(),
            "port": port,
            "username": (username or "").strip() or None,
            "password": (password or "").strip() or None,
            "proxy_url": None,
        }

    parts = token.split(":")
    if len(parts) == 2:
        ip_str, port_str = parts
        try:
            port = int(port_str)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"端口解析失败: {token}") from exc
        return {"ip": ip_str.strip(), "port": port, "username": None, "password": None, "proxy_url": None}

    if len(parts) >= 4:
        ip_str, port_str, username, password = parts[0], parts[1], parts[2], parts[3]
        try:
            port = int(port_str)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"端口解析失败: {token}") from exc
        return {
            "ip": ip_str.strip(),
            "port": port,
            "username": (username or "").strip() or None,
            "password": (password or "").strip() or None,
            "proxy_url": None,
        }

    raise HTTPException(status_code=400, detail=f"不支持的代理格式: {token}")


def _get_system_ip_used_map(db: Session) -> dict:
    rows = (
        db.query(UserScriptEnv.ip_id, func.count(UserScriptEnv.id))
        .filter(
            UserScriptEnv.ip_id.isnot(None),
            UserScriptEnv.status == EnvStatus.VALID.value,
        )
        .group_by(UserScriptEnv.ip_id)
        .all()
    )
    return {ip_id: count for ip_id, count in rows}


@router.get("/ip-pool/admin/list")
async def admin_list_ip_pool(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：查看系统 IP 池（包含过期/禁用/容量/占用情况）"""
    used_map = _get_system_ip_used_map(db)
    ips = db.query(IPPool).order_by(IPPool.id.desc()).all()

    today = date.today()
    data = []
    summary = {
        "total": 0,
        "active": 0,
        "disabled": 0,
        "expired": 0,
        "available": 0,
        "valid_used_total": 0,
        "free_slots_total": 0,
    }

    for ip in ips:
        max_users = ip.max_users or 2
        used = int(used_map.get(ip.id, 0) or 0)
        expired = bool(ip.expire_date and ip.expire_date < today)
        free_slots = max(max_users - used, 0)
        is_available = (ip.status == "active") and (not expired) and (free_slots > 0)

        summary["total"] += 1
        if ip.status == "active":
            summary["active"] += 1
        else:
            summary["disabled"] += 1
        if expired:
            summary["expired"] += 1
        if is_available:
            summary["available"] += 1
        summary["valid_used_total"] += used
        summary["free_slots_total"] += free_slots

        data.append(
            {
                "id": ip.id,
                "ip": ip.ip,
                "port": ip.port,
                "proxy_url": build_proxy_url(ip),
                "proxy_url_raw": ip.proxy_url,
                "region": ip.region,
                "vendor": ip.vendor,
                "expire_date": ip.expire_date,
                "is_expired": expired,
                "status": ip.status,
                "max_users": max_users,
                "used": used,
                "free_slots": free_slots,
                "usage_count": ip.usage_count,
                "created_at": ip.created_at,
                "updated_at": ip.updated_at,
            }
        )

    return {"data": data, "summary": summary}


@router.post("/ip-pool/admin", status_code=status.HTTP_201_CREATED)
async def admin_create_system_ip(
    payload: IPPoolCreatePayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：新增系统 IP"""
    ip_str = (payload.ip or "").strip()
    if not ip_str:
        raise HTTPException(status_code=400, detail="IP 不能为空")
    status_value = _normalize_ip_status_or_400(payload.status) or "active"

    exists = (
        db.query(IPPool.id)
        .filter(IPPool.ip == ip_str, IPPool.port == payload.port)
        .first()
    )
    if exists:
        raise HTTPException(status_code=400, detail="该 IP:端口 已存在")

    record = IPPool(
        ip=ip_str,
        port=payload.port,
        username=(payload.username or "").strip() or None,
        password=(payload.password or "").strip() or None,
        proxy_url=(payload.proxy_url or "").strip() or None,
        region=(payload.region or "").strip() or None,
        vendor=(payload.vendor or "").strip() or None,
        expire_date=payload.expire_date,
        max_users=payload.max_users or 2,
        status=status_value,
        usage_count=0,
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    return {
        "id": record.id,
        "ip": record.ip,
        "port": record.port,
        "proxy_url": build_proxy_url(record),
        "region": record.region,
        "vendor": record.vendor,
        "expire_date": record.expire_date,
        "status": record.status,
        "max_users": record.max_users or 2,
        "used": 0,
        "free_slots": record.max_users or 2,
        "usage_count": record.usage_count,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


@router.put("/ip-pool/admin/{ip_id}")
async def admin_update_system_ip(
    ip_id: int,
    payload: IPPoolUpdatePayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：更新系统 IP（含续期/启用/禁用）"""
    record = db.query(IPPool).filter(IPPool.id == ip_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="IP 不存在")

    if payload.ip is not None:
        ip_str = (payload.ip or "").strip()
        if not ip_str:
            raise HTTPException(status_code=400, detail="IP 不能为空")
        record.ip = ip_str
    if payload.port is not None:
        record.port = payload.port

    if payload.username is not None:
        record.username = (payload.username or "").strip() or None
    if payload.password is not None:
        record.password = (payload.password or "").strip() or None
    if payload.proxy_url is not None:
        record.proxy_url = (payload.proxy_url or "").strip() or None
    if payload.region is not None:
        record.region = (payload.region or "").strip() or None
    if payload.vendor is not None:
        record.vendor = (payload.vendor or "").strip() or None

    if payload.expire_date is not None:
        record.expire_date = payload.expire_date
    if payload.max_users is not None:
        record.max_users = payload.max_users

    if payload.status is not None:
        record.status = _normalize_ip_status_or_400(payload.status) or record.status

    db.commit()
    db.refresh(record)
    return {"message": "更新成功"}


@router.delete("/ip-pool/admin/{ip_id}")
async def admin_delete_system_ip(
    ip_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：删除系统 IP（仅允许无引用时删除）"""
    record = db.query(IPPool).filter(IPPool.id == ip_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="IP 不存在")

    ref_count = (
        db.query(func.count(UserScriptEnv.id))
        .filter(UserScriptEnv.ip_id == ip_id)
        .scalar()
        or 0
    )
    if ref_count:
        raise HTTPException(status_code=400, detail=f"该IP仍被 {ref_count} 个账号引用，不能删除；请先解绑或改为禁用")

    db.delete(record)
    db.commit()
    return {"message": "删除成功"}


@router.post("/ip-pool/admin/import")
async def admin_import_system_ips(
    payload: IPPoolImportPayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：批量导入系统 IP（支持覆盖更新）"""
    text_value = (payload.text or "").strip()
    if not text_value:
        raise HTTPException(status_code=400, detail="导入内容不能为空")

    default_status = _normalize_ip_status_or_400(payload.default_status)
    default_vendor = (payload.default_vendor or "").strip() or None
    default_region = (payload.default_region or "").strip() or None

    created = 0
    updated = 0
    skipped = 0
    failed = 0
    errors: List[dict] = []

    lines = text_value.splitlines()
    for line_no, raw_line in enumerate(lines, start=1):
        line = (raw_line or "").strip()
        if not line:
            continue
        if line.startswith("#") or line.startswith("//") or line.startswith(";"):
            continue
        # 支持行内注释：# 后面的内容忽略
        if "#" in line:
            line = line.split("#", 1)[0].strip()
        if not line:
            continue

        try:
            normalized = line.replace(",", " ")
            tokens = [t for t in normalized.split() if t]
            base = tokens[0] if tokens else ""
            extras = tokens[1:]

            parsed = _parse_system_ip_base_or_400(base)
            expire_date = None
            vendor = None
            region = None
            max_users = None

            if extras and (d := _parse_date_or_none(extras[0])):
                expire_date = d
                extras = extras[1:]
            if extras:
                vendor = extras[0].strip() or None
                extras = extras[1:]
            if extras:
                region = extras[0].strip() or None
                extras = extras[1:]
            if extras:
                try:
                    max_users = int(extras[0])
                except ValueError:
                    max_users = None
            if max_users is not None and not (1 <= max_users <= 20):
                raise HTTPException(status_code=400, detail=f"最大使用人数超出范围(1-20): {max_users}")

            ip_str = (parsed.get("ip") or "").strip()
            port = parsed.get("port")
            if not ip_str or not port:
                raise HTTPException(status_code=400, detail=f"解析失败: {raw_line}")
            if not (1 <= int(port) <= 65535):
                raise HTTPException(status_code=400, detail=f"端口无效: {port}")

            existing = (
                db.query(IPPool)
                .filter(IPPool.ip == ip_str, IPPool.port == int(port))
                .first()
            )

            merged_expire = expire_date or payload.default_expire_date
            merged_vendor = vendor or default_vendor
            merged_region = region or default_region
            merged_max_users = max_users or payload.default_max_users
            if merged_max_users is not None and not (1 <= int(merged_max_users) <= 20):
                raise HTTPException(status_code=400, detail=f"最大使用人数超出范围(1-20): {merged_max_users}")

            if existing:
                if not payload.overwrite:
                    skipped += 1
                    continue

                if parsed.get("username") is not None:
                    existing.username = (parsed.get("username") or "").strip() or None
                if parsed.get("password") is not None:
                    existing.password = (parsed.get("password") or "").strip() or None
                if parsed.get("proxy_url") is not None:
                    existing.proxy_url = (parsed.get("proxy_url") or "").strip() or None

                if merged_expire is not None:
                    existing.expire_date = merged_expire
                if merged_vendor is not None:
                    existing.vendor = merged_vendor
                if merged_region is not None:
                    existing.region = merged_region
                if merged_max_users is not None:
                    existing.max_users = merged_max_users
                if default_status is not None:
                    existing.status = default_status

                updated += 1
                continue

            record = IPPool(
                ip=ip_str,
                port=int(port),
                username=(parsed.get("username") or "").strip() or None,
                password=(parsed.get("password") or "").strip() or None,
                proxy_url=(parsed.get("proxy_url") or "").strip() or None,
                region=merged_region,
                vendor=merged_vendor,
                expire_date=merged_expire,
                max_users=merged_max_users or 2,
                status=default_status or "active",
                usage_count=0,
            )
            db.add(record)
            created += 1
        except Exception as exc:
            failed += 1
            if len(errors) < 50:
                errors.append({"line": line_no, "raw": raw_line, "error": str(exc)})

    db.commit()

    return {
        "message": "导入完成",
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
    }


@router.post("/ip-pool/admin/recalc-usage")
async def admin_recalc_system_ip_usage(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：按当前账号状态重算并写入系统 IP 使用数（usage_count）"""
    recalc_ip_usage(db)
    db.commit()
    return {"message": "重算完成"}


@router.post("/ip-pool/admin/bulk/status")
async def admin_bulk_update_system_ip_status(
    payload: IPPoolBulkStatusPayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：批量启用/禁用系统 IP"""
    ids = sorted(set(int(x) for x in (payload.ids or []) if x))
    if not ids:
        raise HTTPException(status_code=400, detail="请选择至少一个IP")

    status_value = _normalize_ip_status_or_400(payload.status)
    ips = db.query(IPPool).filter(IPPool.id.in_(ids)).all()
    existing_ids = {ip.id for ip in ips}
    missing_ids = [ip_id for ip_id in ids if ip_id not in existing_ids]

    for ip in ips:
        ip.status = status_value

    db.commit()
    return {
        "message": "批量状态更新完成",
        "requested": len(ids),
        "updated": len(ips),
        "status": status_value,
        "missing_ids": missing_ids,
    }


@router.post("/ip-pool/admin/bulk/extend")
async def admin_bulk_extend_system_ip_expire(
    payload: IPPoolBulkExtendPayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：批量续期系统 IP（按天数 +30/+60/+90）"""
    ids = sorted(set(int(x) for x in (payload.ids or []) if x))
    if not ids:
        raise HTTPException(status_code=400, detail="请选择至少一个IP")

    days = int(payload.days)
    if days <= 0:
        raise HTTPException(status_code=400, detail="续期天数必须大于0")

    ips = db.query(IPPool).filter(IPPool.id.in_(ids)).all()
    existing_ids = {ip.id for ip in ips}
    missing_ids = [ip_id for ip_id in ids if ip_id not in existing_ids]

    today = date.today()
    updated_rows: List[dict] = []
    for ip in ips:
        base = ip.expire_date or today
        if payload.from_today_if_expired and base < today:
            base = today
        new_date = base + timedelta(days=days)
        ip.expire_date = new_date
        updated_rows.append({"id": ip.id, "expire_date": str(new_date)})

    db.commit()
    return {
        "message": "批量续期完成",
        "requested": len(ids),
        "updated": len(ips),
        "days": days,
        "missing_ids": missing_ids,
        "updated_rows": updated_rows[:50],
    }


@router.post("/ip-pool/admin/bulk/delete")
async def admin_bulk_delete_system_ips(
    payload: IPPoolBulkIdsPayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员：批量删除系统 IP（仅允许无引用的记录删除）"""
    ids = sorted(set(int(x) for x in (payload.ids or []) if x))
    if not ids:
        raise HTTPException(status_code=400, detail="请选择至少一个IP")

    ips = db.query(IPPool).filter(IPPool.id.in_(ids)).all()
    ip_map = {ip.id: ip for ip in ips}
    existing_ids = sorted(ip_map.keys())
    missing_ids = [ip_id for ip_id in ids if ip_id not in ip_map]

    if not existing_ids:
        return {
            "message": "无可删除记录",
            "requested": len(ids),
            "deleted": 0,
            "blocked": 0,
            "missing_ids": missing_ids,
            "blocked_by_refs": [],
        }

    ref_rows = (
        db.query(UserScriptEnv.ip_id, func.count(UserScriptEnv.id))
        .filter(UserScriptEnv.ip_id.in_(existing_ids))
        .group_by(UserScriptEnv.ip_id)
        .all()
    )
    ref_map = {ip_id: int(count or 0) for ip_id, count in ref_rows}

    blocked_by_refs = [
        {"id": ip_id, "ref_count": ref_map.get(ip_id, 0)}
        for ip_id in existing_ids
        if ref_map.get(ip_id, 0) > 0
    ]
    deletable_ids = [ip_id for ip_id in existing_ids if ref_map.get(ip_id, 0) == 0]

    for ip_id in deletable_ids:
        db.delete(ip_map[ip_id])

    db.commit()
    return {
        "message": "批量删除完成",
        "requested": len(ids),
        "deleted": len(deletable_ids),
        "blocked": len(blocked_by_refs),
        "missing_ids": missing_ids,
        "blocked_by_refs": blocked_by_refs[:50],
    }


class UserIPPoolCreatePayload(BaseModel):
    """新增用户自有代理"""
    ip: str = Field(..., description="IP")
    port: int = Field(..., ge=1, le=65535, description="端口")
    username: str = Field(..., description="代理账号")
    password: str = Field(..., description="代理密码")
    region: Optional[str] = Field(None, description="地区/城市")
    vendor: Optional[str] = Field(None, description="供应商")
    expire_date: Optional[date] = Field(None, description="到期时间")
    max_users: Optional[int] = Field(None, ge=1, le=20, description="最多同时使用人数（默认2）")


@router.get("/user-ip-pool/available")
async def list_available_user_ips(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """获取某用户自有代理列表（包含容量信息）"""
    if not can_manage_user(current_user, user_id, db):
        raise HTTPException(status_code=403, detail="无权管理此用户")

    ips = (
        db.query(UserIPPool)
        .filter(
            UserIPPool.user_id == user_id,
            UserIPPool.status == "active",
            (UserIPPool.expire_date.is_(None)) | (UserIPPool.expire_date >= date.today()),
        )
        .order_by(UserIPPool.id.desc())
        .all()
    )

    ip_ids = [ip.id for ip in ips]
    usage_map = {}
    if ip_ids:
        usage_rows = (
            db.query(UserScriptEnv.user_ip_id, func.count(UserScriptEnv.id))
            .filter(
                UserScriptEnv.user_ip_id.in_(ip_ids),
                UserScriptEnv.status == EnvStatus.VALID.value,
            )
            .group_by(UserScriptEnv.user_ip_id)
            .all()
        )
        usage_map = {ip_id: int(count or 0) for ip_id, count in usage_rows}

    available = []
    for ip in ips:
        used = usage_map.get(ip.id, 0)
        available.append(
            {
                "id": ip.id,
                "proxy_url": build_user_proxy_url(ip),
                "region": ip.region,
                "vendor": ip.vendor,
                "max_users": ip.max_users or 2,
                "used": used,
                "usage_count": used,
            }
        )
    return {"data": available}


@router.post("/users/{user_id}/user-ip-pool", status_code=status.HTTP_201_CREATED)
async def create_user_ip_pool(
    user_id: int,
    data: UserIPPoolCreatePayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """为某用户新增自有代理（自动拼接 socks5://username:password@ip:port）"""
    if not can_manage_user(current_user, user_id, db):
        raise HTTPException(status_code=403, detail="无权管理此用户")

    ip = (data.ip or "").strip()
    username = (data.username or "").strip()
    password = (data.password or "").strip()
    if not ip:
        raise HTTPException(status_code=400, detail="IP 不能为空")
    if not username or not password:
        raise HTTPException(status_code=400, detail="代理账号/密码不能为空")

    proxy_url = f"socks5://{username}:{password}@{ip}:{data.port}"
    record = UserIPPool(
        user_id=user_id,
        ip=ip,
        port=data.port,
        username=username,
        password=password,
        proxy_url=proxy_url,
        region=(data.region or "").strip() or None,
        vendor=(data.vendor or "").strip() or None,
        expire_date=data.expire_date,
        max_users=data.max_users or 2,
        status="active",
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    recalc_user_ip_usage(db, {record.id})

    used = record.usage_count or 0
    return {
        "id": record.id,
        "proxy_url": build_user_proxy_url(record),
        "region": record.region,
        "vendor": record.vendor,
        "max_users": record.max_users or 2,
        "used": used,
        "usage_count": used,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


@router.get(
    "/configs/{config_id}/envs", response_model=List[UserScriptEnvResponse]
)
async def list_envs(
    config_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """查看配置下的环境变量（含下级权限）"""
    config = get_config_or_404(config_id, db)
    assert_config_permission(current_user, config, db)
    envs = (
        db.query(UserScriptEnv)
        .filter(
            UserScriptEnv.config_id == config_id,
            ~UserScriptEnv.env_name.like("__archived__%")
        )
        .all()
    )

    system_ip_ids = {env.ip_id for env in envs if env.ip_id}
    user_ip_ids = {env.user_ip_id for env in envs if env.user_ip_id}

    system_ip_map = {}
    user_ip_map = {}
    system_usage_map = {}
    user_usage_map = {}

    if system_ip_ids:
        system_ip_map = {
            ip.id: ip
            for ip in db.query(IPPool).filter(IPPool.id.in_(system_ip_ids)).all()
        }
        usage_rows = (
            db.query(UserScriptEnv.ip_id, func.count(UserScriptEnv.id))
            .filter(
                UserScriptEnv.ip_id.in_(system_ip_ids),
                UserScriptEnv.status == EnvStatus.VALID.value,
            )
            .group_by(UserScriptEnv.ip_id)
            .all()
        )
        system_usage_map = {ip_id: int(count or 0) for ip_id, count in usage_rows}

    if user_ip_ids:
        user_ip_map = {
            ip.id: ip
            for ip in db.query(UserIPPool).filter(UserIPPool.id.in_(user_ip_ids)).all()
        }
        usage_rows = (
            db.query(UserScriptEnv.user_ip_id, func.count(UserScriptEnv.id))
            .filter(
                UserScriptEnv.user_ip_id.in_(user_ip_ids),
                UserScriptEnv.status == EnvStatus.VALID.value,
            )
            .group_by(UserScriptEnv.user_ip_id)
            .all()
        )
        user_usage_map = {ip_id: int(count or 0) for ip_id, count in usage_rows}

    result = []
    for env in envs:
        mode = (env.ip_mode or IP_MODE_SYSTEM_RANDOM).strip()
        if mode not in VALID_IP_MODES:
            mode = IP_MODE_SYSTEM_RANDOM

        ip = system_ip_map.get(env.ip_id) if env.ip_id else None
        user_ip = user_ip_map.get(env.user_ip_id) if env.user_ip_id else None

        ip_info = None
        user_ip_info = None

        if mode == IP_MODE_USER_POOL and user_ip:
            user_ip_info = {
                "id": user_ip.id,
                "proxy_url": build_user_proxy_url(user_ip),
                "region": user_ip.region,
                "vendor": user_ip.vendor,
                "max_users": user_ip.max_users or 2,
                "used": user_usage_map.get(user_ip.id, 0),
            }
        elif ip:
            ip_info = {
                "id": ip.id,
                "proxy_url": build_proxy_url(ip),
                "region": ip.region,
                "vendor": ip.vendor,
                "max_users": ip.max_users or 2,
                "used": system_usage_map.get(ip.id, 0),
            }
        result.append(
            {
                "id": env.id,
                "config_id": env.config_id,
                "env_name": env.env_name,
                "env_value": env.env_value,
                "ql_env_id": env.ql_env_id,
                "ip_mode": mode,
                "ip_id": env.ip_id,
                "ip_info": ip_info,
                "user_ip_id": env.user_ip_id,
                "user_ip_info": user_ip_info,
                "status": env.status,
                "remark": env.remark,
                "disabled_until": env.disabled_until.isoformat() if env.disabled_until else None,
                "created_at": env.created_at,
                "updated_at": env.updated_at,
            }
        )
    return result


@router.post(
    "/configs/{config_id}/envs",
    response_model=UserScriptEnvResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_env(
    config_id: int,
    data: KSCKEnvPayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """新增环境变量"""
    config = get_config_or_404(config_id, db)
    assert_config_permission(current_user, config, db)

    if not can_create_env(current_user, config.user_id, db):
        raise HTTPException(status_code=403, detail="无权为该用户新增环境变量")
    cookie = normalize_cookie_or_400(data.cookie)
    remark = normalize_remark_or_400(data.remark)
    assert_unique_remark(db, remark)

    ip_mode = normalize_ip_mode_or_default(data.ip_mode)
    env_status = data.status or EnvStatus.VALID.value
    if env_status not in (EnvStatus.VALID.value, EnvStatus.INVALID.value):
        raise HTTPException(status_code=400, detail="状态仅支持 valid/invalid")

    system_ip_obj: Optional[IPPool] = None
    user_ip_obj: Optional[UserIPPool] = None
    proxy_url = ""

    if ip_mode == IP_MODE_USER_POOL:
        if data.user_ip_id is None:
            raise HTTPException(status_code=400, detail="请选择自有代理")
        user_ip_obj = get_user_ip_with_usage(db, config.user_id, data.user_ip_id)
        proxy_url = build_user_proxy_url(user_ip_obj)
        env = UserScriptEnv(
            config_id=config_id,
            user_id=config.user_id,
            env_name=generate_env_name(db, config_id),
            env_value=cookie,
            ip_mode=ip_mode,
            ip_id=None,
            user_ip_id=user_ip_obj.id,
            status=env_status,
            remark=remark,
        )
    else:
        if data.ip_id is not None:
            raise HTTPException(status_code=400, detail="系统 IP 模式下不支持手动选择 IP")
        if env_status == EnvStatus.VALID.value:
            system_ip_obj = pick_random_system_ip(db)
            proxy_url = build_proxy_url(system_ip_obj)
            desired_ip_id = system_ip_obj.id
        else:
            system_ip_obj = None
            proxy_url = ""
            desired_ip_id = None
        env = UserScriptEnv(
            config_id=config_id,
            user_id=config.user_id,
            env_name=generate_env_name(db, config_id),
            env_value=cookie,
            ip_mode=ip_mode,
            ip_id=desired_ip_id,
            user_ip_id=None,
            status=env_status,
            remark=remark,
        )

    db.add(env)
    db.commit()
    db.refresh(env)

    if system_ip_obj:
        recalc_ip_usage(db, {system_ip_obj.id})
    if user_ip_obj:
        recalc_user_ip_usage(db, {user_ip_obj.id})

    # 尝试同步到青龙
    try:
        client = get_ql_client_for_config(config, db)
        ql_id = sync_env_to_ql(
            client,
            env,
            config_id,
            enable=env.status == EnvStatus.VALID.value,
            proxy_url=proxy_url,
        )
        env.ql_env_id = ql_id
        config.last_sync_at = datetime.now()
        db.commit()
        db.refresh(env)
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"保存成功但同步青龙失败: {exc}")

    ip_info = None
    user_ip_info = None
    if system_ip_obj:
        used_count = (
            db.query(func.count(UserScriptEnv.id))
            .filter(
                UserScriptEnv.ip_id == system_ip_obj.id,
                UserScriptEnv.status == EnvStatus.VALID.value,
            )
            .scalar()
            or 0
        )
        ip_info = {
            "id": system_ip_obj.id,
            "proxy_url": build_proxy_url(system_ip_obj),
            "region": system_ip_obj.region,
            "vendor": system_ip_obj.vendor,
            "max_users": system_ip_obj.max_users or 2,
            "used": used_count,
            "usage_count": used_count,
        }

    if user_ip_obj:
        used_count = (
            db.query(func.count(UserScriptEnv.id))
            .filter(
                UserScriptEnv.user_ip_id == user_ip_obj.id,
                UserScriptEnv.status == EnvStatus.VALID.value,
            )
            .scalar()
            or 0
        )
        user_ip_info = {
            "id": user_ip_obj.id,
            "proxy_url": build_user_proxy_url(user_ip_obj),
            "region": user_ip_obj.region,
            "vendor": user_ip_obj.vendor,
            "max_users": user_ip_obj.max_users or 2,
            "used": used_count,
            "usage_count": used_count,
        }

    return {
        "id": env.id,
        "config_id": env.config_id,
        "env_name": env.env_name,
        "env_value": env.env_value,
        "ql_env_id": env.ql_env_id,
        "ip_mode": ip_mode,
        "ip_id": env.ip_id,
        "ip_info": ip_info,
        "user_ip_id": env.user_ip_id,
        "user_ip_info": user_ip_info,
        "status": env.status,
        "remark": env.remark,
        "disabled_until": env.disabled_until.isoformat() if env.disabled_until else None,
        "created_at": env.created_at,
        "updated_at": env.updated_at,
    }


@router.put(
    "/configs/{config_id}/envs/{env_id}",
    response_model=UserScriptEnvResponse,
)
async def update_env(
    config_id: int,
    env_id: int,
    data: KSCKEnvPayload,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """修改环境变量（如果已同步，将同时更新青龙）"""
    logger = get_logger(__name__)
    config = get_config_or_404(config_id, db)
    assert_config_permission(current_user, config, db)
    env = get_env_or_404(env_id, config_id, db)

    cookie = normalize_cookie_or_400(
        data.cookie if data.cookie is not None else env.env_value
    )
    remark = normalize_remark_or_400(
        data.remark if data.remark is not None else env.remark
    )
    assert_unique_remark(db, remark, exclude_env_id=env.id)

    old_remark = (env.remark or "").strip()
    if old_remark and remark != old_remark:
        used_in_earnings = db.query(EarningRecord).filter(EarningRecord.env_id == env.id).first()
        if used_in_earnings:
            raise HTTPException(status_code=400, detail="备注已用于收益统计，不能修改")

    env.env_value = cookie
    env.remark = remark
    if data.status is not None:
        if data.status not in (EnvStatus.VALID.value, EnvStatus.INVALID.value):
            raise HTTPException(status_code=400, detail="状态仅支持 valid/invalid")
        env.status = data.status

    old_ip_id = env.ip_id
    old_user_ip_id = env.user_ip_id
    old_mode = (env.ip_mode or IP_MODE_SYSTEM_RANDOM).strip()
    if old_mode not in VALID_IP_MODES:
        old_mode = IP_MODE_SYSTEM_RANDOM

    ip_mode = normalize_ip_mode_or_default(data.ip_mode if data.ip_mode is not None else old_mode)

    system_ip_obj: Optional[IPPool] = None
    user_ip_obj: Optional[UserIPPool] = None
    proxy_url = ""

    if ip_mode == IP_MODE_USER_POOL:
        user_ip_id = data.user_ip_id if data.user_ip_id is not None else env.user_ip_id
        if user_ip_id is None:
            raise HTTPException(status_code=400, detail="请选择自有代理")
        user_ip_obj = get_user_ip_with_usage(
            db,
            user_id=config.user_id,
            user_ip_id=user_ip_id,
            exclude_env_id=env.id,
        )
        proxy_url = build_user_proxy_url(user_ip_obj)
        env.ip_mode = ip_mode
        env.ip_id = None
        env.user_ip_id = user_ip_id
    else:
        if data.ip_id is not None:
            raise HTTPException(status_code=400, detail="系统 IP 模式下不支持手动选择 IP")

        desired_ip_id = env.ip_id
        if env.status == EnvStatus.VALID.value:
            if desired_ip_id is not None:
                try:
                    system_ip_obj = get_ip_with_usage(db, desired_ip_id, exclude_env_id=env.id)
                except HTTPException:
                    system_ip_obj = pick_random_system_ip(db, exclude_env_id=env.id)
                    desired_ip_id = system_ip_obj.id
            else:
                system_ip_obj = pick_random_system_ip(db, exclude_env_id=env.id)
                desired_ip_id = system_ip_obj.id
            proxy_url = build_proxy_url(system_ip_obj)
        else:
            desired_ip_id = None
            proxy_url = ""
        env.ip_mode = ip_mode
        env.ip_id = desired_ip_id
        env.user_ip_id = None

    # 同步到青龙（无论是否已有 ql_env_id）
    try:
        client = get_ql_client_for_config(config, db)
        old_ql_env_id = env.ql_env_id
        ql_id = sync_env_to_ql(
            client,
            env,
            config_id,
            enable=env.status == EnvStatus.VALID.value,
            proxy_url=proxy_url,
        )
        if old_ql_env_id and str(old_ql_env_id) != str(ql_id):
            logger.warning(
                "青龙变量ID已变更: env_name=%s, old_ql_env_id=%s, new_ql_env_id=%s",
                env.env_name,
                old_ql_env_id,
                ql_id,
            )
        env.ql_env_id = ql_id
        logger.info("同步到青龙成功: env_name=%s, ql_env_id=%s", env.env_name, env.ql_env_id)

        config.last_sync_at = datetime.now()
        db.commit()
        db.refresh(env)
    except Exception as exc:
        db.rollback()
        logger.error(f"同步青龙失败: env_id={env_id}, env_name={env.env_name}, error={exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"同步青龙失败: {exc}"
        )

    system_ids_to_recalc: Set[int] = set()
    if old_ip_id:
        system_ids_to_recalc.add(old_ip_id)
    if env.ip_id:
        system_ids_to_recalc.add(env.ip_id)
    if system_ids_to_recalc:
        recalc_ip_usage(db, system_ids_to_recalc)

    user_ids_to_recalc: Set[int] = set()
    if old_user_ip_id:
        user_ids_to_recalc.add(old_user_ip_id)
    if env.user_ip_id:
        user_ids_to_recalc.add(env.user_ip_id)
    if user_ids_to_recalc:
        recalc_user_ip_usage(db, user_ids_to_recalc)

    ip_info = None
    user_ip_info = None
    current_ip_mode = (env.ip_mode or IP_MODE_SYSTEM_RANDOM).strip()
    if current_ip_mode not in VALID_IP_MODES:
        current_ip_mode = IP_MODE_SYSTEM_RANDOM

    if current_ip_mode == IP_MODE_USER_POOL and env.user_ip_id:
        current_user_ip = user_ip_obj
        if not current_user_ip or current_user_ip.id != env.user_ip_id:
            current_user_ip = (
                db.query(UserIPPool).filter(UserIPPool.id == env.user_ip_id).first()
            )
        if current_user_ip:
            used_count = (
                db.query(func.count(UserScriptEnv.id))
                .filter(
                    UserScriptEnv.user_ip_id == current_user_ip.id,
                    UserScriptEnv.status == EnvStatus.VALID.value,
                )
                .scalar()
                or 0
            )
            user_ip_info = {
                "id": current_user_ip.id,
                "proxy_url": build_user_proxy_url(current_user_ip),
                "region": current_user_ip.region,
                "vendor": current_user_ip.vendor,
                "max_users": current_user_ip.max_users or 2,
                "used": used_count,
            }
    elif env.ip_id:
        current_ip = system_ip_obj
        if not current_ip or current_ip.id != env.ip_id:
            current_ip = db.query(IPPool).filter(IPPool.id == env.ip_id).first()
        if current_ip:
            used_count = (
                db.query(func.count(UserScriptEnv.id))
                .filter(
                    UserScriptEnv.ip_id == current_ip.id,
                    UserScriptEnv.status == EnvStatus.VALID.value,
                )
                .scalar()
                or 0
            )
            ip_info = {
                "id": current_ip.id,
                "proxy_url": build_proxy_url(current_ip),
                "region": current_ip.region,
                "vendor": current_ip.vendor,
                "max_users": current_ip.max_users or 2,
                "used": used_count,
            }

    return {
        "id": env.id,
        "config_id": env.config_id,
        "env_name": env.env_name,
        "env_value": env.env_value,
        "ql_env_id": env.ql_env_id,
        "ip_mode": current_ip_mode,
        "ip_id": env.ip_id,
        "ip_info": ip_info,
        "user_ip_id": env.user_ip_id,
        "user_ip_info": user_ip_info,
        "status": env.status,
        "remark": env.remark,
        "disabled_until": env.disabled_until.isoformat() if env.disabled_until else None,
        "created_at": env.created_at,
        "updated_at": env.updated_at,
    }


@router.delete("/configs/{config_id}/envs/{env_id}")
async def delete_env(
    config_id: int,
    env_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """删除环境变量（若青龙ID存在则一并删除）"""
    config = get_config_or_404(config_id, db)
    assert_config_permission(current_user, config, db)
    env = get_env_or_404(env_id, config_id, db)

    used_in_earnings = db.query(EarningRecord).filter(EarningRecord.env_id == env.id).first()
    if used_in_earnings:
        raise HTTPException(status_code=400, detail="该账号已存在收益记录，不能删除；请改为禁用")

    try:
        if env.ql_env_id:
            client = get_ql_client_for_config(config, db)
            client.delete_env(env.ql_env_id)
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"删除青龙变量失败: {exc}")

    system_ip_ids = {env.ip_id} if env.ip_id else set()
    user_ip_ids = {env.user_ip_id} if env.user_ip_id else set()

    db.delete(env)
    db.commit()
    if system_ip_ids:
        recalc_ip_usage(db, system_ip_ids)
    if user_ip_ids:
        recalc_user_ip_usage(db, user_ip_ids)
    return {"message": "删除成功"}


@router.post("/configs/{config_id}/envs/{env_id}/enable")
async def enable_env(
    config_id: int,
    env_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """启用环境变量并同步到青龙"""
    config = get_config_or_404(config_id, db)
    assert_config_permission(current_user, config, db)
    env = get_env_or_404(env_id, config_id, db)
    client = get_ql_client_for_config(config, db)
    mode = (env.ip_mode or IP_MODE_SYSTEM_RANDOM).strip()
    if mode not in VALID_IP_MODES:
        mode = IP_MODE_SYSTEM_RANDOM

    proxy_url = ""
    if mode == IP_MODE_USER_POOL:
        if not env.user_ip_id:
            raise HTTPException(status_code=400, detail="该环境未配置自有代理")
        user_ip_obj = get_user_ip_with_usage(
            db,
            user_id=config.user_id,
            user_ip_id=env.user_ip_id,
            exclude_env_id=env.id,
        )
        proxy_url = build_user_proxy_url(user_ip_obj)
    else:
        ip_obj = pick_random_system_ip(db, exclude_env_id=env.id)
        env.ip_mode = IP_MODE_SYSTEM_RANDOM
        env.ip_id = ip_obj.id
        env.user_ip_id = None
        proxy_url = build_proxy_url(ip_obj)

    try:
        env.ql_env_id = sync_env_to_ql(client, env, config_id, enable=True, proxy_url=proxy_url)

        if not env.ql_env_id:
            raise HTTPException(status_code=500, detail="同步青龙失败，缺少ID")

        env.status = EnvStatus.VALID.value
        config.last_sync_at = datetime.now()
        db.commit()
        if env.ip_id:
            recalc_ip_usage(db, {env.ip_id})
        if env.user_ip_id:
            recalc_user_ip_usage(db, {env.user_ip_id})
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"启用失败: {exc}")

    return {"message": "已启用", "ql_env_id": env.ql_env_id}


@router.post("/configs/{config_id}/envs/{env_id}/disable")
async def disable_env(
    config_id: int,
    env_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """禁用环境变量并同步到青龙"""
    config = get_config_or_404(config_id, db)
    assert_config_permission(current_user, config, db)
    env = get_env_or_404(env_id, config_id, db)
    if not env.ql_env_id:
        raise HTTPException(status_code=400, detail="该变量尚未同步到青龙")

    client = get_ql_client_for_config(config, db)
    try:
        old_ip_id = env.ip_id
        old_user_ip_id = env.user_ip_id

        mode = (env.ip_mode or IP_MODE_SYSTEM_RANDOM).strip()
        if mode not in VALID_IP_MODES:
            mode = IP_MODE_SYSTEM_RANDOM
        proxy_url = (
            build_user_proxy_url(env.user_ip) if mode == IP_MODE_USER_POOL else build_proxy_url(env.ip)
        )
        env.ql_env_id = sync_env_to_ql(client, env, config_id, enable=False, proxy_url=proxy_url)
        env.status = EnvStatus.INVALID.value
        if mode != IP_MODE_USER_POOL:
            env.ip_id = None
            env.user_ip_id = None
        config.last_sync_at = datetime.now()
        db.commit()
        if old_ip_id:
            recalc_ip_usage(db, {old_ip_id})
        if old_user_ip_id:
            recalc_user_ip_usage(db, {old_user_ip_id})
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"禁用失败: {exc}")

    return {"message": "已禁用", "ql_env_id": env.ql_env_id}
