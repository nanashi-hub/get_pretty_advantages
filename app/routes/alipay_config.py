"""
支付宝配置管理路由（管理员）
"""
import os
import uuid
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field, validator
from typing import List, Optional
from decimal import Decimal

from app.database import get_db
from app.models import AlipayConfig, User, UserRole
from app.auth import get_current_user

router = APIRouter(prefix="/api/admin/alipay", tags=["支付宝配置"])

# 收款码上传目录
UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "uploads" / "qrcode"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# 允许的图片格式
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


# ==================== Schemas ====================

class AlipayConfigCreate(BaseModel):
    """创建支付宝配置请求"""
    name: str = Field(..., min_length=1, max_length=50, description="配置名称")
    app_id: str = Field(..., min_length=1, max_length=50, description="支付宝应用ID")
    private_key: str = Field(..., min_length=1, description="应用私钥")
    alipay_public_key: str = Field(..., min_length=1, description="支付宝公钥")
    sign_type: str = Field(default="RSA2", description="签名方式 RSA/RSA2")
    gateway: str = Field(default="https://openapi.alipay.com/gateway.do", description="网关地址")
    qrcode_url: Optional[str] = Field(None, max_length=255, description="收款码图片URL")
    alipay_account: Optional[str] = Field(None, max_length=100, description="平台支付宝账号")

    # 分账配置
    platform_fee_rate: Decimal = Field(default=Decimal("0.1000"), ge=0, le=1, description="平台抽成比例")
    agent_l1_rate: Decimal = Field(default=Decimal("0.5400"), ge=0, le=1, description="一级代理分成比例")
    agent_l2_rate: Decimal = Field(default=Decimal("0.2700"), ge=0, le=1, description="二级代理分成比例")
    user_rate: Decimal = Field(default=Decimal("0.0900"), ge=0, le=1, description="号主分成比例")

    remark: Optional[str] = Field(None, max_length=255, description="备注")

    @validator('private_key', 'alipay_public_key')
    def validate_pem_format(cls, v):
        """验证PEM格式"""
        if not v.startswith('-----'):
            # 如果不是标准PEM格式，尝试添加
            return v.strip()
        return v.strip()


class AlipayConfigUpdate(BaseModel):
    """更新支付宝配置请求"""
    name: Optional[str] = Field(None, min_length=1, max_length=50)
    app_id: Optional[str] = Field(None, min_length=1, max_length=50)
    private_key: Optional[str] = None
    alipay_public_key: Optional[str] = None
    sign_type: Optional[str] = None
    gateway: Optional[str] = None
    qrcode_url: Optional[str] = None
    alipay_account: Optional[str] = None

    platform_fee_rate: Optional[Decimal] = None
    agent_l1_rate: Optional[Decimal] = None
    agent_l2_rate: Optional[Decimal] = None
    user_rate: Optional[Decimal] = None

    status: Optional[int] = Field(None, ge=0, le=1)
    remark: Optional[str] = None


class AlipayConfigResponse(BaseModel):
    """支付宝配置响应"""
    id: int
    name: str
    app_id: str
    sign_type: str
    gateway: str
    qrcode_url: Optional[str] = None
    alipay_account: Optional[str] = None

    platform_fee_rate: float
    agent_l1_rate: float
    agent_l2_rate: float
    user_rate: float

    status: int
    remark: Optional[str] = None
    created_at: str
    updated_at: str

    # 不返回敏感信息
    private_key: Optional[str] = None
    alipay_public_key: Optional[str] = None

    class Config:
        from_attributes = True


# ==================== API 接口 ====================

@router.post("/configs", response_model=AlipayConfigResponse)
async def create_alipay_config(
    data: AlipayConfigCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """创建支付宝配置（管理员）"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可操作"
        )

    # 检查 app_id 是否已存在
    existing = db.query(AlipayConfig).filter(
        AlipayConfig.app_id == data.app_id
    ).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="该应用ID已存在"
        )

    # 处理密钥格式（确保是完整的PEM格式）
    private_key_pem = _ensure_pem_format(data.private_key, "PRIVATE KEY")
    alipay_public_key_pem = _ensure_pem_format(data.alipay_public_key, "PUBLIC KEY")

    config = AlipayConfig(
        name=data.name,
        app_id=data.app_id,
        private_key=private_key_pem,
        alipay_public_key=alipay_public_key_pem,
        sign_type=data.sign_type,
        gateway=data.gateway,
        qrcode_url=data.qrcode_url,
        alipay_account=data.alipay_account,
        platform_fee_rate=data.platform_fee_rate,
        agent_l1_rate=data.agent_l1_rate,
        agent_l2_rate=data.agent_l2_rate,
        user_rate=data.user_rate,
        status=1,
        remark=data.remark
    )

    db.add(config)
    db.commit()
    db.refresh(config)

    return _format_config_response(config)


@router.get("/configs", response_model=List[AlipayConfigResponse])
async def list_alipay_configs(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取支付宝配置列表（管理员）"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问"
        )

    configs = db.query(AlipayConfig).order_by(
        AlipayConfig.status.desc(),
        AlipayConfig.id.desc()
    ).all()

    return [_format_config_response(c) for c in configs]


@router.get("/configs/{config_id}", response_model=AlipayConfigResponse)
async def get_alipay_config(
    config_id: int,
    include_secrets: bool = False,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取支付宝配置详情（管理员）"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问"
        )

    config = db.query(AlipayConfig).filter(
        AlipayConfig.id == config_id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="配置不存在"
        )

    response = _format_config_response(config)
    if include_secrets:
        response.private_key = config.private_key
        response.alipay_public_key = config.alipay_public_key

    return response


@router.put("/configs/{config_id}", response_model=AlipayConfigResponse)
async def update_alipay_config(
    config_id: int,
    data: AlipayConfigUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """更新支付宝配置（管理员）"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可操作"
        )

    config = db.query(AlipayConfig).filter(
        AlipayConfig.id == config_id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="配置不存在"
        )

    update_data = data.model_dump(exclude_unset=True)

    # 处理密钥格式
    if 'private_key' in update_data and update_data['private_key']:
        update_data['private_key'] = _ensure_pem_format(update_data['private_key'], "PRIVATE KEY")

    if 'alipay_public_key' in update_data and update_data['alipay_public_key']:
        update_data['alipay_public_key'] = _ensure_pem_format(update_data['alipay_public_key'], "PUBLIC KEY")

    for key, value in update_data.items():
        setattr(config, key, value)

    db.commit()
    db.refresh(config)

    return _format_config_response(config)


@router.delete("/configs/{config_id}")
async def delete_alipay_config(
    config_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """删除支付宝配置（管理员）"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可操作"
        )

    config = db.query(AlipayConfig).filter(
        AlipayConfig.id == config_id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="配置不存在"
        )

    db.delete(config)
    db.commit()

    return {"message": "已删除"}


@router.post("/configs/{config_id}/enable")
async def enable_alipay_config(
    config_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """启用支付宝配置（管理员）"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可操作"
        )

    config = db.query(AlipayConfig).filter(
        AlipayConfig.id == config_id
    ).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="配置不存在"
        )

    # 禁用其他配置
    db.query(AlipayConfig).filter(
        AlipayConfig.id != config_id
    ).update({"status": 0})

    # 启用当前配置
    config.status = 1
    db.commit()

    return {"message": "已启用"}


# ==================== 工具函数 ====================

def _ensure_pem_format(key: str, key_type: str) -> str:
    """确保密钥是标准的PEM格式"""
    key = key.strip()

    # 如果已经有PEM头尾，直接返回
    if key.startswith('-----BEGIN') and key.endswith('-----END'):
        return key

    # 添加PEM头尾
    if key_type == "PRIVATE KEY":
        if not key.startswith('-----BEGIN'):
            key = f"-----BEGIN PRIVATE KEY-----\n{key}\n-----END PRIVATE KEY-----"
    elif key_type == "PUBLIC KEY":
        if not key.startswith('-----BEGIN'):
            key = f"-----BEGIN PUBLIC KEY-----\n{key}\n-----END PUBLIC KEY-----"

    return key


def _format_config_response(config: AlipayConfig) -> AlipayConfigResponse:
    """格式化配置响应（隐藏敏感信息）"""
    return AlipayConfigResponse(
        id=config.id,
        name=config.name,
        app_id=config.app_id,
        sign_type=config.sign_type,
        gateway=config.gateway,
        qrcode_url=config.qrcode_url,
        alipay_account=config.alipay_account,
        platform_fee_rate=float(config.platform_fee_rate),
        agent_l1_rate=float(config.agent_l1_rate),
        agent_l2_rate=float(config.agent_l2_rate),
        user_rate=float(config.user_rate),
        status=config.status,
        remark=config.remark,
        created_at=config.created_at.isoformat() if config.created_at else None,
        updated_at=config.updated_at.isoformat() if config.updated_at else None
    )


# ==================== 收款码图片上传接口 ====================

@router.post("/upload-qrcode")
async def upload_qrcode(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user)
):
    """
    上传收款码图片
    - 仅管理员可操作
    - 支持 jpg, jpeg, png, gif, webp 格式
    - 文件保存到 data/uploads/qrcode/ 目录
    - 返回文件访问路径
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可操作"
        )

    # 检查文件类型
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="请选择要上传的文件"
        )

    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"不支持的文件格式，仅支持: {', '.join(ALLOWED_EXTENSIONS)}"
        )

    # 检查文件大小（限制5MB）
    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="文件大小不能超过5MB"
        )

    # 生成唯一文件名：timestamp_uuid.ext
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    new_filename = f"{timestamp}_{unique_id}{file_ext}"
    file_path = UPLOAD_DIR / new_filename

    # 保存文件
    try:
        with open(file_path, "wb") as f:
            f.write(content)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"文件保存失败: {str(e)}"
        )

    # 返回访问路径（相对路径，供前端使用）
    return {
        "success": True,
        "filename": new_filename,
        "url": f"/api/admin/alipay/qrcode/{new_filename}",
        "message": "上传成功"
    }


@router.get("/qrcode/{filename}")
async def get_qrcode(filename: str):
    """
    获取收款码图片
    - 此接口不需要登录即可访问（用于前端显示）
    - 验证文件名格式防止路径遍历攻击
    """
    # 安全检查：防止路径遍历
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="无效的文件名"
        )

    # 检查文件扩展名
    file_ext = Path(filename).suffix.lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="不支持的文件格式"
        )

    file_path = UPLOAD_DIR / filename

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="文件不存在"
        )

    # 根据扩展名确定媒体类型
    media_types = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp"
    }
    media_type = media_types.get(file_ext, "application/octet-stream")

    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename
    )


@router.get("/qrcode-list")
async def list_qrcodes(
    current_user: User = Depends(get_current_user)
):
    """
    获取已上传的收款码图片列表（管理员）
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问"
        )

    files = []
    if UPLOAD_DIR.exists():
        for file_path in UPLOAD_DIR.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in ALLOWED_EXTENSIONS:
                stat = file_path.stat()
                files.append({
                    "filename": file_path.name,
                    "url": f"/api/admin/alipay/qrcode/{file_path.name}",
                    "size": stat.st_size,
                    "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat()
                })

    # 按创建时间倒序排列
    files.sort(key=lambda x: x["created_at"], reverse=True)

    return {"files": files}


@router.delete("/qrcode/{filename}")
async def delete_qrcode(
    filename: str,
    current_user: User = Depends(get_current_user)
):
    """
    删除收款码图片（管理员）
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可操作"
        )

    # 安全检查：防止路径遍历
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="无效的文件名"
        )

    file_path = UPLOAD_DIR / filename

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="文件不存在"
        )

    try:
        file_path.unlink()
        return {"success": True, "message": "删除成功"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除失败: {str(e)}"
        )
