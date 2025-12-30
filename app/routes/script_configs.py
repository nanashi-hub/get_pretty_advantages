from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta
from app.database import get_db
from app.models import UserScriptConfig, UserScriptEnv, QLInstance, User, UserRole
from app.schemas import (
    UserScriptConfigCreate, UserScriptConfigUpdate, UserScriptConfigResponse,
    UserScriptEnvCreate, UserScriptEnvUpdate, UserScriptEnvResponse, EnvDisableRequest
)
from app.auth import get_current_user
from app.services.qinglong import QingLongClient

router = APIRouter(prefix="/api", tags=["脚本配置"])


def get_ql_client(db: Session, ql_instance_id: int) -> QingLongClient:
    """获取青龙客户端"""
    instance = db.query(QLInstance).filter(QLInstance.id == ql_instance_id).first()
    if not instance:
        raise HTTPException(status_code=404, detail="青龙实例不存在")
    if instance.status != 1:
        raise HTTPException(status_code=400, detail="青龙实例已停用")
    return QingLongClient(instance)


# ==================== 脚本配置 CRUD ====================

@router.get("/script-configs", response_model=List[UserScriptConfigResponse])
async def get_script_configs(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """获取脚本配置列表"""
    query = db.query(UserScriptConfig)
    
    # 非管理员只能看自己的配置
    if current_user.role != UserRole.ADMIN:
        query = query.filter(UserScriptConfig.user_id == current_user.id)
    
    return query.order_by(UserScriptConfig.id.desc()).all()


@router.get("/script-configs/{config_id}", response_model=UserScriptConfigResponse)
async def get_script_config(
    config_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """获取单个脚本配置"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权访问此配置")
    
    return config


@router.post("/script-configs", response_model=UserScriptConfigResponse, status_code=status.HTTP_201_CREATED)
async def create_script_config(
    data: UserScriptConfigCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """创建脚本配置"""
    # 管理员可以指定 user_id；未指定时默认当前用户
    if current_user.role == UserRole.ADMIN:
        user_id = data.user_id or current_user.id
    else:
        user_id = current_user.id
    
    config = UserScriptConfig(
        user_id=user_id,
        ql_instance_id=data.ql_instance_id,
        script_name=data.script_name,
        group_key=data.group_key,
        status=data.status
    )
    db.add(config)
    db.commit()
    db.refresh(config)
    return config


@router.put("/script-configs/{config_id}", response_model=UserScriptConfigResponse)
async def update_script_config(
    config_id: int,
    data: UserScriptConfigUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """更新脚本配置"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权修改此配置")
    
    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(config, key, value)
    
    db.commit()
    db.refresh(config)
    return config


@router.delete("/script-configs/{config_id}")
async def delete_script_config(
    config_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """删除脚本配置"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权删除此配置")
    
    db.delete(config)
    db.commit()
    return {"message": "删除成功"}


# ==================== 环境变量管理 ====================

@router.get("/script-configs/{config_id}/envs", response_model=List[UserScriptEnvResponse])
async def get_config_envs(
    config_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """获取配置的环境变量"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权访问此配置")
    
    envs = db.query(UserScriptEnv).filter(UserScriptEnv.config_id == config_id).all()
    return envs


@router.post("/script-configs/{config_id}/envs", response_model=UserScriptEnvResponse, status_code=status.HTTP_201_CREATED)
async def create_config_env(
    config_id: int,
    data: UserScriptEnvCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """创建环境变量"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权操作此配置")
    
    env = UserScriptEnv(
        config_id=config_id,
        env_name=data.env_name,
        env_value=data.env_value,
        ql_env_id=data.ql_env_id,
        status=data.status,
        remark=data.remark
    )
    db.add(env)
    db.commit()
    db.refresh(env)
    return env


@router.post("/script-configs/{config_id}/envs/batch")
async def batch_save_envs(
    config_id: int,
    envs_data: List[dict],
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """批量保存环境变量"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权操作此配置")
    
    # 删除旧的环境变量
    db.query(UserScriptEnv).filter(UserScriptEnv.config_id == config_id).delete()
    
    # 创建新的环境变量
    for env_data in envs_data:
        env = UserScriptEnv(
            config_id=config_id,
            env_name=env_data.get('env_name'),
            env_value=env_data.get('env_value'),
            ql_env_id=env_data.get('ql_env_id'),
            status=env_data.get('status', 'valid'),
            remark=env_data.get('remark')
        )
        db.add(env)
    
    db.commit()
    return {"message": "保存成功"}


# ==================== 青龙同步功能 ====================

@router.post("/script-configs/{config_id}/sync")
async def sync_to_ql(
    config_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """同步配置到青龙（创建/更新环境变量）"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权操作此配置")
    
    # 获取青龙客户端
    try:
        client = get_ql_client(db, config.ql_instance_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"连接青龙失败: {e}")
    
    # 获取配置下的所有环境变量
    envs = db.query(UserScriptEnv).filter(UserScriptEnv.config_id == config_id).all()
    if not envs:
        raise HTTPException(status_code=400, detail="没有环境变量需要同步")
    
    results = []
    errors = []
    
    for env in envs:
        try:
            # 同步到青龙
            enabled = env.status == 'valid'
            result = client.sync_env(
                name=env.env_name,
                value=env.env_value,
                remarks=env.remark or f"配置ID:{config_id}",
                enabled=enabled
            )
            
            # 更新本地的 ql_env_id
            ql_env_id = result.get("id") or result.get("_id")
            if ql_env_id:
                env.ql_env_id = str(ql_env_id)
            
            results.append({"env_name": env.env_name, "status": "success", "ql_env_id": ql_env_id})
        except Exception as e:
            errors.append({"env_name": env.env_name, "status": "error", "message": str(e)})
    
    # 更新同步时间
    config.last_sync_at = datetime.now()
    db.commit()
    
    return {
        "message": f"同步完成，成功 {len(results)} 个，失败 {len(errors)} 个",
        "success": results,
        "errors": errors
    }


@router.post("/script-configs/{config_id}/envs/{env_id}/sync")
async def sync_single_env_to_ql(
    config_id: int,
    env_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """同步单个环境变量到青龙"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权操作此配置")
    
    env = db.query(UserScriptEnv).filter(
        UserScriptEnv.id == env_id,
        UserScriptEnv.config_id == config_id
    ).first()
    if not env:
        raise HTTPException(status_code=404, detail="环境变量不存在")
    
    try:
        client = get_ql_client(db, config.ql_instance_id)
        enabled = env.status == 'valid'
        result = client.sync_env(
            name=env.env_name,
            value=env.env_value,
            remarks=env.remark or f"配置ID:{config_id}",
            enabled=enabled
        )
        
        # 更新本地的 ql_env_id
        ql_env_id = result.get("id") or result.get("_id")
        if ql_env_id:
            env.ql_env_id = str(ql_env_id)
            db.commit()
        
        return {"message": "同步成功", "ql_env_id": ql_env_id, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"同步失败: {e}")


@router.post("/script-configs/{config_id}/envs/{env_id}/enable")
async def enable_env_in_ql(
    config_id: int,
    env_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """启用青龙环境变量"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")

    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权操作此配置")

    env = db.query(UserScriptEnv).filter(
        UserScriptEnv.id == env_id,
        UserScriptEnv.config_id == config_id
    ).first()
    if not env:
        raise HTTPException(status_code=404, detail="环境变量不存在")

    if not env.ql_env_id:
        raise HTTPException(status_code=400, detail="该变量尚未同步到青龙，请先同步")

    try:
        client = get_ql_client(db, config.ql_instance_id)
        client.enable_env(env.ql_env_id)

        # 更新本地状态，清除禁用恢复字段
        env.status = 'valid'
        env.disabled_until = None
        env.disable_days = None
        env.disabled_at = None
        db.commit()

        return {"message": "启用成功"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启用失败: {e}")


@router.post("/script-configs/{config_id}/envs/{env_id}/disable")
async def disable_env_in_ql(
    config_id: int,
    env_id: int,
    request_data: EnvDisableRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """禁用青龙环境变量，支持指定天数后自动恢复"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")

    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权操作此配置")

    env = db.query(UserScriptEnv).filter(
        UserScriptEnv.id == env_id,
        UserScriptEnv.config_id == config_id
    ).first()
    if not env:
        raise HTTPException(status_code=404, detail="环境变量不存在")

    if not env.ql_env_id:
        raise HTTPException(status_code=400, detail="该变量尚未同步到青龙，请先同步")

    try:
        client = get_ql_client(db, config.ql_instance_id)
        client.disable_env(env.ql_env_id)

        # 更新本地状态和禁用恢复时间
        now = datetime.now()
        disabled_until = now + timedelta(days=request_data.days)

        env.status = 'invalid'
        env.disable_days = request_data.days
        env.disabled_at = now
        env.disabled_until = disabled_until
        db.commit()

        return {
            "message": f"禁用成功，将在{request_data.days}天后自动恢复",
            "disabled_until": disabled_until.isoformat(),
            "days": request_data.days
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"禁用失败: {e}")


@router.delete("/script-configs/{config_id}/envs/{env_id}/ql")
async def delete_env_from_ql(
    config_id: int,
    env_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """从青龙删除环境变量（仅删除青龙上的，保留本地记录）"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")
    
    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权操作此配置")
    
    env = db.query(UserScriptEnv).filter(
        UserScriptEnv.id == env_id,
        UserScriptEnv.config_id == config_id
    ).first()
    if not env:
        raise HTTPException(status_code=404, detail="环境变量不存在")
    
    if not env.ql_env_id:
        raise HTTPException(status_code=400, detail="该变量尚未同步到青龙")
    
    try:
        client = get_ql_client(db, config.ql_instance_id)
        client.delete_env(env.ql_env_id)
        
        # 清除本地的 ql_env_id
        env.ql_env_id = None
        db.commit()
        
        return {"message": "已从青龙删除"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除失败: {e}")


# ==================== 从青龙拉取环境变量 ====================

@router.get("/script-configs/{config_id}/ql-envs")
async def list_ql_envs(
    config_id: int,
    search: str = "",
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """查询青龙上的环境变量列表"""
    config = db.query(UserScriptConfig).filter(UserScriptConfig.id == config_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="配置不存在")

    if current_user.role != UserRole.ADMIN and config.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="无权操作此配置")

    try:
        client = get_ql_client(db, config.ql_instance_id)
        envs = client.list_envs(search_value=search)
        return {"data": envs, "total": len(envs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {e}")


# ==================== 自动恢复禁用的环境变量 ====================

@router.post("/script-configs/auto-restore-disabled")
async def auto_restore_disabled_envs(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    自动恢复禁用到期的环境变量
    可由定时任务（如 APScheduler）定期调用
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="仅管理员可执行")

    now = datetime.now()

    # 查找所有已过期且仍处于禁用状态的环境变量
    expired_envs = db.query(UserScriptEnv).filter(
        UserScriptEnv.status == 'invalid',
        UserScriptEnv.disabled_until.isnot(None),
        UserScriptEnv.disabled_until <= now
    ).all()

    restored_count = 0
    failed_count = 0
    results = []

    for env in expired_envs:
        try:
            config = env.config
            if not config or not config.ql_instance_id:
                results.append({"env_id": env.id, "status": "skip", "reason": "无配置或青龙实例"})
                continue

            client = get_ql_client(db, config.ql_instance_id)

            if env.ql_env_id:
                # 在青龙中启用
                client.enable_env(env.ql_env_id)

            # 更新本地状态
            env.status = 'valid'
            env.disabled_until = None
            env.disable_days = None
            env.disabled_at = None
            db.commit()

            restored_count += 1
            results.append({
                "env_id": env.id,
                "env_name": env.env_name,
                "status": "restored",
                "disabled_days": env.disable_days
            })
        except Exception as e:
            failed_count += 1
            results.append({
                "env_id": env.id,
                "env_name": env.env_name,
                "status": "failed",
                "error": str(e)
            })

    return {
        "message": f"恢复完成: {restored_count}个成功, {failed_count}个失败",
        "restored_count": restored_count,
        "failed_count": failed_count,
        "details": results
    }


@router.get("/script-configs/disabled-pending")
async def get_disabled_pending_envs(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """获取即将自动恢复的禁用环境变量列表"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="仅管理员可查看")

    now = datetime.now()

    # 查找所有禁用中且未过期的环境变量
    pending_envs = db.query(UserScriptEnv).filter(
        UserScriptEnv.status == 'invalid',
        UserScriptEnv.disabled_until.isnot(None),
        UserScriptEnv.disabled_until > now
    ).all()

    results = []
    for env in pending_envs:
        results.append({
            "id": env.id,
            "env_name": env.env_name,
            "config_id": env.config_id,
            "user_id": env.config.user_id if env.config else None,
            "disabled_at": env.disabled_at.isoformat() if env.disabled_at else None,
            "disabled_until": env.disabled_until.isoformat() if env.disabled_until else None,
            "disable_days": env.disable_days,
            "remaining_hours": int((env.disabled_until - now).total_seconds() / 3600) if env.disabled_until else None
        })

    # 按剩余时间排序
    results.sort(key=lambda x: x["remaining_hours"] or 0)

    return {"data": results, "total": len(results)}
