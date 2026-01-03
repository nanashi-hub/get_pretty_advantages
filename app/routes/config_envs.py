from datetime import date, datetime
from typing import List, Optional, Set

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
from app.services.qinglong import QingLongClient

router = APIRouter(prefix="/api/config-envs", tags=["配置环境"])

DEFAULT_QL_NAME = "默认青龙实例"
DEFAULT_QL_BASE_URL = "http://192.168.5.204:1116"
DEFAULT_QL_CLIENT_ID = "N16sNCmXwY_S"
DEFAULT_QL_CLIENT_SECRET = "rr_tBarvo4lwvDnbzKyJhq2j"
DEFAULT_QL_REMARK = "自动创建的默认青龙实例（来自配置环境模块）"


class KSCKEnvPayload(BaseModel):
    """新增/修改 ksck 变量的载荷"""
    cookie: Optional[str] = Field(None, description="ksck 值（必填）")
    remark: Optional[str] = Field(None, description="备注")
    ip_id: Optional[int] = Field(None, description="IP池ID")
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


def build_ql_value(env: UserScriptEnv, ip: Optional[IPPool]) -> str:
    """按 备注#cookie#proxy_url 组合青龙变量值"""
    remark = env.remark or ""
    cookie = env.env_value or ""
    proxy_url = build_proxy_url(ip)
    return f"{remark}#{cookie}#{proxy_url}"


def recalc_ip_usage(db: Session, ip_ids: Optional[Set[int]] = None) -> None:
    """刷新 IP 使用次数到 ip_pool.usage_count（不使用触发器）"""
    # 统计当前使用数
    usage_query = db.query(UserScriptEnv.ip_id, func.count(UserScriptEnv.id)).filter(
        UserScriptEnv.ip_id.isnot(None)
    )
    if ip_ids:
        usage_query = usage_query.filter(UserScriptEnv.ip_id.in_(ip_ids))
    usage_rows = usage_query.group_by(UserScriptEnv.ip_id).all()
    usage_map = {ip_id: count for ip_id, count in usage_rows}

    targets = ip_ids or set(usage_map.keys())
    if not targets:
        return
    for ip_id in targets:
        db.query(IPPool).filter(IPPool.id == ip_id).update(
            {"usage_count": usage_map.get(ip_id, 0)}
        )
    db.flush()


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
    """管理员可为任意用户新增；普通用户仅可为下级新增，不能为自己新增"""
    if current_user.role == UserRole.ADMIN:
        return True
    if target_user_id == current_user.id:
        return False
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
        UserScriptEnv.ip_id == ip_id
    )
    if exclude_env_id:
        usage_query = usage_query.filter(UserScriptEnv.id != exclude_env_id)
    used = usage_query.scalar() or 0
    if used >= ip.max_users:
        raise HTTPException(status_code=400, detail="该IP使用已达上限")
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
        .options(joinedload(UserScriptEnv.ip))  # 预加载 IP 关联
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
    ip: Optional[IPPool] = None,
) -> str:
    """同步本地环境变量到青龙并返回青龙ID"""
    ql_value = build_ql_value(env, ip or env.ip)
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
    """获取剩余容量的IP池列表"""
    recalc_ip_usage(db)

    ips = (
        db.query(IPPool)
        .filter(
            IPPool.status == "active",
            (IPPool.expire_date.is_(None)) | (IPPool.expire_date >= date.today()),
        )
        .order_by(IPPool.id.desc())
        .all()
    )

    available = []
    for ip in ips:
        used = ip.usage_count or 0
        if used < (ip.max_users or 2):
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
    envs = db.query(UserScriptEnv).filter(UserScriptEnv.config_id == config_id).all()

    ip_ids = [env.ip_id for env in envs if env.ip_id]
    ip_map = {}
    if ip_ids:
        ip_map = {
            ip.id: ip
            for ip in db.query(IPPool).filter(IPPool.id.in_(set(ip_ids))).all()
        }
        recalc_ip_usage(db, set(ip_ids))

    result = []
    for env in envs:
        ip = ip_map.get(env.ip_id) if env.ip_id else None
        ip_info = None
        if ip:
            ip_info = {
                "id": ip.id,
                "proxy_url": build_proxy_url(ip),
                "region": ip.region,
                "vendor": ip.vendor,
                "max_users": ip.max_users or 2,
                "used": ip.usage_count or 0,
            }
        result.append(
            {
                "id": env.id,
                "config_id": env.config_id,
                "env_name": env.env_name,
                "env_value": env.env_value,
                "ql_env_id": env.ql_env_id,
                "ip_id": env.ip_id,
                "ip_info": ip_info,
                "status": env.status,
                "remark": env.remark,
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
    if current_user.role == UserRole.NORMAL and current_user.id == config.user_id:
        raise HTTPException(status_code=403, detail="普通用户无法新增环境变量")
    cookie = normalize_cookie_or_400(data.cookie)
    remark = normalize_remark_or_400(data.remark)
    if data.ip_id is None:
        raise HTTPException(status_code=400, detail="请选择IP")
    assert_unique_remark(db, remark)

    ip_obj = get_ip_with_usage(db, data.ip_id)

    env = UserScriptEnv(
        config_id=config_id,
        user_id=config.user_id,
        env_name=generate_env_name(db, config_id),
        env_value=cookie,
        ip_id=data.ip_id,
        status=data.status or EnvStatus.VALID.value,
        remark=remark,
    )
    db.add(env)
    db.commit()
    db.refresh(env)
    recalc_ip_usage(db, {data.ip_id})

    # 尝试同步到青龙
    try:
        client = get_ql_client_for_config(config, db)
        ql_id = sync_env_to_ql(
            client,
            env,
            config_id,
            enable=env.status == EnvStatus.VALID.value,
            ip=ip_obj,
        )
        env.ql_env_id = ql_id
        config.last_sync_at = datetime.now()
        db.commit()
        db.refresh(env)
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"保存成功但同步青龙失败: {exc}")

    used_count = ip_obj.usage_count if ip_obj else 0

    return {
        "id": env.id,
        "config_id": env.config_id,
        "env_name": env.env_name,
        "env_value": env.env_value,
        "ql_env_id": env.ql_env_id,
        "ip_id": env.ip_id,
        "ip_info": {
            "id": ip_obj.id,
            "proxy_url": build_proxy_url(ip_obj),
            "region": ip_obj.region,
            "vendor": ip_obj.vendor,
            "max_users": ip_obj.max_users or 2,
            "used": used_count,
            "usage_count": used_count,
        }
        if ip_obj
        else None,
        "status": env.status,
        "remark": env.remark,
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

    cookie = normalize_cookie_or_400(data.cookie if data.cookie is not None else env.env_value)
    remark = normalize_remark_or_400(data.remark if data.remark is not None else env.remark)
    ip_id = data.ip_id if data.ip_id is not None else env.ip_id
    if ip_id is None:
        raise HTTPException(status_code=400, detail="请选择IP")
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
    ip_obj = get_ip_with_usage(db, ip_id, exclude_env_id=env.id)
    env.ip_id = ip_id

    # 同步到青龙（无论是否已有 ql_env_id）
    try:
        client = get_ql_client_for_config(config, db)
        old_ql_env_id = env.ql_env_id
        ql_id = sync_env_to_ql(
            client,
            env,
            config_id,
            enable=env.status == EnvStatus.VALID.value,
            ip=ip_obj,
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

    ip_ids_to_recalc: Set[int] = set()
    if old_ip_id:
        ip_ids_to_recalc.add(old_ip_id)
    if env.ip_id:
        ip_ids_to_recalc.add(env.ip_id)
    if ip_ids_to_recalc:
        recalc_ip_usage(db, ip_ids_to_recalc)

    # 获取当前 IP 使用情况
    used_count = 0
    current_ip_obj = None
    if env.ip_id:
        current_ip_obj = (
            db.query(IPPool)
            .filter(IPPool.id == env.ip_id)
            .first()
        )
        if current_ip_obj:
            used_count = current_ip_obj.usage_count or 0

    return {
        "id": env.id,
        "config_id": env.config_id,
        "env_name": env.env_name,
        "env_value": env.env_value,
        "ql_env_id": env.ql_env_id,
        "ip_id": env.ip_id,
        "ip_info": {
            "id": current_ip_obj.id,
            "proxy_url": build_proxy_url(current_ip_obj),
            "region": current_ip_obj.region,
            "vendor": current_ip_obj.vendor,
            "max_users": current_ip_obj.max_users or 2,
            "used": used_count,
        }
        if current_ip_obj
        else None,
        "status": env.status,
        "remark": env.remark,
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

    db.delete(env)
    db.commit()
    if env.ip_id:
        recalc_ip_usage(db, {env.ip_id})
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
    ip_obj = get_ip_with_usage(db, env.ip_id, exclude_env_id=env.id) if env.ip_id else None

    try:
        env.ql_env_id = sync_env_to_ql(client, env, config_id, enable=True, ip=ip_obj)

        if not env.ql_env_id:
            raise HTTPException(status_code=500, detail="同步青龙失败，缺少ID")

        env.status = EnvStatus.VALID.value
        config.last_sync_at = datetime.now()
        db.commit()
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
        env.ql_env_id = sync_env_to_ql(client, env, config_id, enable=False)
        env.status = EnvStatus.INVALID.value
        config.last_sync_at = datetime.now()
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"禁用失败: {exc}")

    return {"message": "已禁用", "ql_env_id": env.ql_env_id}
