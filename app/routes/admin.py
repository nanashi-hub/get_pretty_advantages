"""
管理员相关路由
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List
from app.database import get_db
from app.models import User, UserRole, UserScriptConfig, UserScriptEnv
from app.schemas import UserRegister, UserResponse, UserUpdate
from app.auth import hash_password, get_current_user
from datetime import timedelta
from app.auth import create_access_token
from app.schemas import Token

router = APIRouter(prefix="/api/admin", tags=["管理员"])


@router.post("/create-admin", response_model=Token)
async def create_admin_account(
    user_data: UserRegister,
    admin_secret: str,  # 管理员密钥，用于安全创建管理员
    db: Session = Depends(get_db)
):
    """
    创建管理员账号（需要管理员密钥）
    管理员密钥可以在环境变量 ADMIN_SECRET 中设置，默认: ADMIN_SECRET_KEY_2024
    """
    import os
    expected_secret = os.getenv("ADMIN_SECRET", "ADMIN_SECRET_KEY_2024")
    
    if admin_secret != expected_secret:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="无效的管理员密钥"
        )
    
    # 检查用户名是否已存在
    existing_user = db.query(User).filter(User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="用户名已存在"
        )
    
    # 检查是否已有管理员
    admin_exists = db.query(User).filter(User.role == UserRole.ADMIN).first()
    if admin_exists and not os.getenv("ALLOW_MULTIPLE_ADMINS", "false").lower() == "true":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="管理员已存在，如需创建多个管理员，请设置环境变量 ALLOW_MULTIPLE_ADMINS=true"
        )
    
    # 创建管理员
    hashed_password = hash_password(user_data.password)
    admin_user = User(
        username=user_data.username,
        password_hash=hashed_password,
        nickname=user_data.nickname,
        phone=user_data.phone,
        wechat_id=user_data.wechat_id,
        role=UserRole.ADMIN,
        status=1
    )
    
    db.add(admin_user)
    db.commit()
    db.refresh(admin_user)
    
    # 生成访问令牌
    access_token_expires = timedelta(minutes=30)
    access_token = create_access_token(
        data={"sub": admin_user.username},
        expires_delta=access_token_expires
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": UserResponse.model_validate(admin_user)
    }


@router.get("/users")
async def list_all_users(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取所有用户列表（仅管理员），包含每个用户的ksck数量"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可以访问此接口"
        )

    users = db.query(User).all()
    result = []

    for user in users:
        user_data = UserResponse.model_validate(user).model_dump()

        # 获取用户的ksck列表
        configs = db.query(UserScriptConfig).filter(UserScriptConfig.user_id == user.id).all()
        ksck_list = []
        for config in configs:
            envs = db.query(UserScriptEnv).filter(UserScriptEnv.config_id == config.id).all()
            for env in envs:
                # 将status转换为字符串值，处理Enum对象
                status_str = env.status.value if hasattr(env.status, 'value') else str(env.status)
                ksck_list.append({
                    "id": env.id,
                    "name": env.env_name,
                    "status": status_str
                })

        user_data["ksck_count"] = len(ksck_list)
        user_data["ksck_list"] = ksck_list
        result.append(user_data)

    return result


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    data: UserUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """更新用户信息/状态（仅管理员）"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="仅管理员可操作")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")

    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(user, key, value)
    db.commit()
    db.refresh(user)
    return UserResponse.model_validate(user)


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """删除用户（仅管理员，不可删除自己）"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="仅管理员可操作")
    if user_id == current_user.id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="不能删除自己")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")

    # 导入所有需要的模型
    from app.models import (
        UserReferral, WalletAccount, WalletTransaction,
        UserScriptConfig, UserScriptEnv
    )

    # 1. 删除钱包交易记录
    try:
        db.query(WalletTransaction).filter(WalletTransaction.user_id == user_id).delete()
    except Exception:
        pass  # 如果列名不对，跳过

    # 2. 删除钱包账户
    db.query(WalletAccount).filter(WalletAccount.user_id == user_id).delete()

    # 3. 删除推广关系记录
    db.query(UserReferral).filter(UserReferral.user_id == user_id).delete()
    db.query(UserReferral).filter(UserReferral.inviter_level1 == user_id).delete()
    db.query(UserReferral).filter(UserReferral.inviter_level2 == user_id).delete()

    # 4. 删除脚本配置和环境变量
    # 注意：QLInstance 是全局共享资源，不属于特定用户，无需在此删除
    configs = db.query(UserScriptConfig).filter(UserScriptConfig.user_id == user_id).all()
    for config in configs:
        db.query(UserScriptEnv).filter(UserScriptEnv.config_id == config.id).delete()
        db.delete(config)

    # 6. 最后删除用户
    db.delete(user)
    db.commit()
    return {"message": "已删除"}

