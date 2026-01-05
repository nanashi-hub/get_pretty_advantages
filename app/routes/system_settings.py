from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import get_current_user
from app.database import get_db
from app.models import SystemSetting, User, UserRole


router = APIRouter(prefix="/api", tags=["系统设置"])

ServiceMode = Literal["commercial", "public"]


class ServiceModeResponse(BaseModel):
    service_mode: ServiceMode = Field(..., description="commercial=商业版 public=公益版")


class ServiceModeUpdate(BaseModel):
    service_mode: ServiceMode = Field(..., description="commercial=商业版 public=公益版")


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """要求管理员权限"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="需要管理员权限")
    return current_user


def _get_service_mode(db: Session) -> ServiceMode:
    setting = db.query(SystemSetting).filter(SystemSetting.setting_key == "service_mode").first()
    if not setting or not setting.setting_value:
        return "commercial"
    return "public" if setting.setting_value == "public" else "commercial"


@router.get("/service-mode", response_model=ServiceModeResponse)
async def get_service_mode(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """获取当前服务模式（登录用户可读）"""
    _ = current_user
    return ServiceModeResponse(service_mode=_get_service_mode(db))


@router.post("/admin/service-mode", response_model=ServiceModeResponse)
async def set_service_mode(
    data: ServiceModeUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """设置服务模式（管理员）"""
    _ = current_user
    setting = db.query(SystemSetting).filter(SystemSetting.setting_key == "service_mode").first()
    if setting:
        setting.setting_value = data.service_mode
    else:
        db.add(SystemSetting(setting_key="service_mode", setting_value=data.service_mode))
    db.commit()
    return ServiceModeResponse(service_mode=data.service_mode)

