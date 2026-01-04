from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from app.database import get_db
from app.models import UserReferral, User, UserRole
from app.auth import get_current_user

router = APIRouter(prefix="/api", tags=["推广关系"])


@router.get("/referrals")
async def get_referrals(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """获取推广关系列表"""
    if current_user.role == UserRole.ADMIN:
        # 管理员可以看所有
        referrals = db.query(UserReferral).all()
    else:
        # 普通用户只能看自己相关的
        referrals = db.query(UserReferral).filter(
            (UserReferral.user_id == current_user.id) |
            (UserReferral.inviter_level1 == current_user.id) |
            (UserReferral.inviter_level2 == current_user.id)
        ).all()
    
    return [
        {
            "user_id": r.user_id,
            "inviter_level1": r.inviter_level1,
            "inviter_level2": r.inviter_level2,
            "created_at": r.created_at
        }
        for r in referrals
    ]


@router.get("/referrals/my-invites")
async def get_my_invites(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """获取我邀请的用户（含完整用户信息）"""
    # 直接邀请（+1）
    level1_referrals = db.query(UserReferral).filter(
        UserReferral.inviter_level1 == current_user.id
    ).all()

    # 间接邀请（+2）
    level2_referrals = db.query(UserReferral).filter(
        UserReferral.inviter_level2 == current_user.id
    ).all()

    # 获取用户ID列表
    level1_user_ids = [r.user_id for r in level1_referrals]
    level2_user_ids = [r.user_id for r in level2_referrals]

    # 批量获取用户信息
    all_user_ids = list(set(level1_user_ids + level2_user_ids))
    user_map = {}
    if all_user_ids:
        users = db.query(User).filter(User.id.in_(all_user_ids)).all()
        user_map = {u.id: u for u in users}

    # 构建返回数据，包含完整用户信息
    level1_users = []
    for user_id in level1_user_ids:
        user = user_map.get(user_id)
        if user:
            level1_users.append({
                "id": user.id,
                "username": user.username,
                "nickname": user.nickname,
                "role": user.role.value if hasattr(user.role, "value") else user.role,
                "status": user.status,
                "created_at": user.created_at.isoformat() if user.created_at else None
            })

    level2_users = []
    for user_id in level2_user_ids:
        user = user_map.get(user_id)
        if user:
            level2_users.append({
                "id": user.id,
                "username": user.username,
                "nickname": user.nickname,
                "role": user.role.value if hasattr(user.role, "value") else user.role,
                "status": user.status,
                "created_at": user.created_at.isoformat() if user.created_at else None
            })

    return {
        "level1_count": len(level1_users),
        "level2_count": len(level2_users),
        "level1_users": level1_users,
        "level2_users": level2_users
    }


@router.get("/referrals/chain/{user_id}")
async def get_referral_chain(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """获取用户的推广链路"""
    # 只有管理员可以查看任意用户，普通用户只能查看自己
    if current_user.role != UserRole.ADMIN and current_user.id != user_id:
        raise HTTPException(status_code=403, detail="无权查看")
    
    referral = db.query(UserReferral).filter(UserReferral.user_id == user_id).first()
    
    if not referral:
        return {"user_id": user_id, "inviter_level1": None, "inviter_level2": None}
    
    # 获取邀请人信息
    inviter1 = None
    inviter2 = None
    
    if referral.inviter_level1:
        user1 = db.query(User).filter(User.id == referral.inviter_level1).first()
        if user1:
            inviter1 = {"id": user1.id, "username": user1.username, "nickname": user1.nickname}
    
    if referral.inviter_level2:
        user2 = db.query(User).filter(User.id == referral.inviter_level2).first()
        if user2:
            inviter2 = {"id": user2.id, "username": user2.username, "nickname": user2.nickname}
    
    return {
        "user_id": user_id,
        "inviter_level1": inviter1,
        "inviter_level2": inviter2,
        "created_at": referral.created_at
    }
