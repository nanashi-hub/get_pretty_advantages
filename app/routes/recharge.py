"""
充值订单相关路由
"""
from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field, validator
from typing import List, Optional
from decimal import Decimal
from datetime import datetime, timedelta

from app.database import get_db
from app.models import (
    User, RechargeOrder, TransferRecord,
    RechargeOrderStatus, TransferStatus, UserRole
)
from app.auth import get_current_user
from app.services.alipay_service import (
    get_alipay_config, generate_order_no, check_pending_payments,
    distribute_amount, get_wallet_with_alipay, manually_confirm_payment
)

router = APIRouter(prefix="/api/recharge", tags=["充值订单"])


# ==================== Schemas ====================

class RechargeOrderCreate(BaseModel):
    """创建充值订单请求"""
    amount: Decimal = Field(..., gt=0, description="充值金额")
    remark_in: Optional[str] = Field(None, max_length=200, description="付款备注")

    @validator('amount')
    def validate_amount(cls, v):
        """金额验证：保留两位小数"""
        return v.quantize(Decimal('0.01'))


class RechargeOrderResponse(BaseModel):
    """充值订单响应"""
    id: int
    order_no: str
    user_id: int
    amount: float
    status: str
    alipay_trade_no: Optional[str] = None
    paid_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    expired_at: Optional[datetime] = None
    created_at: datetime
    qrcode_url: Optional[str] = None  # 支付宝收款码

    class Config:
        from_attributes = True


class TransferRecordResponse(BaseModel):
    """转账记录响应"""
    id: int
    recharge_order_id: int
    user_id: int
    amount: float
    role: str
    alipay_account: str
    alipay_order_id: Optional[str] = None
    status: str
    fail_reason: Optional[str] = None
    transferred_at: Optional[datetime] = None
    created_at: datetime

    class Config:
        from_attributes = True


class RechargeOrderDetail(BaseModel):
    """充值订单详情（包含转账记录）"""
    order: RechargeOrderResponse
    transfers: List[TransferRecordResponse]


class AlipayConfigResponse(BaseModel):
    """支付宝配置响应（仅返回必要信息）"""
    id: int
    name: str
    qrcode_url: Optional[str] = None
    platform_fee_rate: float
    agent_l1_rate: float
    agent_l2_rate: float
    user_rate: float

    class Config:
        from_attributes = True


# ==================== API 接口 ====================

@router.post("/orders", response_model=RechargeOrderResponse)
async def create_recharge_order(
    data: RechargeOrderCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    创建充值订单

    流程：
    1. 生成唯一订单号
    2. 创建订单记录（状态：pending）
    3. 返回订单信息和收款二维码
    """
    # 检查支付宝配置
    alipay_config = get_alipay_config(db)
    if not alipay_config:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="支付宝配置未设置，请联系管理员"
        )

    # 检查是否有未完成的订单（防止重复创建）
    pending_order = db.query(RechargeOrder).filter(
        RechargeOrder.user_id == current_user.id,
        RechargeOrder.status == RechargeOrderStatus.PENDING,
        RechargeOrder.expired_at > datetime.now()
    ).first()

    if pending_order:
        # 返回现有订单
        return RechargeOrderResponse(
            id=pending_order.id,
            order_no=pending_order.order_no,
            user_id=pending_order.user_id,
            amount=float(pending_order.amount),
            status=pending_order.status.value,
            alipay_trade_no=pending_order.alipay_trade_no,
            paid_at=pending_order.paid_at,
            confirmed_at=pending_order.confirmed_at,
            expired_at=pending_order.expired_at,
            created_at=pending_order.created_at,
            qrcode_url=alipay_config.qrcode_url
        )

    # 生成订单号
    order_no = generate_order_no()

    # 订单过期时间：30分钟
    expired_at = datetime.now() + timedelta(minutes=30)

    # 创建订单
    order = RechargeOrder(
        order_no=order_no,
        user_id=current_user.id,
        amount=data.amount,
        remark_in=data.remark_in,
        status=RechargeOrderStatus.PENDING,
        expired_at=expired_at
    )

    db.add(order)
    db.commit()
    db.refresh(order)

    return RechargeOrderResponse(
        id=order.id,
        order_no=order.order_no,
        user_id=order.user_id,
        amount=float(order.amount),
        status=order.status.value,
        alipay_trade_no=order.alipay_trade_no,
        paid_at=order.paid_at,
        confirmed_at=order.confirmed_at,
        expired_at=order.expired_at,
        created_at=order.created_at,
        qrcode_url=alipay_config.qrcode_url
    )


@router.get("/orders", response_model=List[RechargeOrderResponse])
async def list_recharge_orders(
    status_filter: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取当前用户的充值订单列表

    status_filter: 可选状态筛选 (pending/paid/confirmed/cancelled/expired)
    """
    query = db.query(RechargeOrder).filter(
        RechargeOrder.user_id == current_user.id
    )

    if status_filter:
        try:
            status_enum = RechargeOrderStatus(status_filter)
            query = query.filter(RechargeOrder.status == status_enum)
        except ValueError:
            pass  # 忽略无效状态

    orders = query.order_by(RechargeOrder.created_at.desc()).all()

    # 获取收款码URL
    alipay_config = get_alipay_config(db)
    qrcode_url = alipay_config.qrcode_url if alipay_config else None

    return [
        RechargeOrderResponse(
            id=o.id,
            order_no=o.order_no,
            user_id=o.user_id,
            amount=float(o.amount),
            status=o.status.value,
            alipay_trade_no=o.alipay_trade_no,
            paid_at=o.paid_at,
            confirmed_at=o.confirmed_at,
            expired_at=o.expired_at,
            created_at=o.created_at,
            qrcode_url=qrcode_url
        )
        for o in orders
    ]


@router.get("/orders/{order_no}", response_model=RechargeOrderDetail)
async def get_recharge_order(
    order_no: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取充值订单详情（包含转账记录）"""
    order = db.query(RechargeOrder).filter(
        RechargeOrder.order_no == order_no,
        RechargeOrder.user_id == current_user.id
    ).first()

    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="订单不存在"
        )

    # 获取转账记录
    transfers = db.query(TransferRecord).filter(
        TransferRecord.recharge_order_id == order.id
    ).all()

    return RechargeOrderDetail(
        order=RechargeOrderResponse(
            id=order.id,
            order_no=order.order_no,
            user_id=order.user_id,
            amount=float(order.amount),
            status=order.status.value,
            alipay_trade_no=order.alipay_trade_no,
            paid_at=order.paid_at,
            confirmed_at=order.confirmed_at,
            expired_at=order.expired_at,
            created_at=order.created_at
        ),
        transfers=[
            TransferRecordResponse(
                id=t.id,
                recharge_order_id=t.recharge_order_id,
                user_id=t.user_id,
                amount=float(t.amount),
                role=t.role,
                alipay_account=t.alipay_account,
                alipay_order_id=t.alipay_order_id,
                status=t.status.value,
                fail_reason=t.fail_reason,
                transferred_at=t.transferred_at,
                created_at=t.created_at
            )
            for t in transfers
        ]
    )


@router.post("/orders/{order_no}/check")
async def check_order_payment(
    order_no: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    手动检查订单支付状态

    调用支付宝接口查询交易记录，确认是否已支付
    """
    order = db.query(RechargeOrder).filter(
        RechargeOrder.order_no == order_no,
        RechargeOrder.user_id == current_user.id
    ).first()

    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="订单不存在"
        )

    if order.status != RechargeOrderStatus.PENDING:
        return {
            "order_no": order_no,
            "status": order.status.value,
            "message": "订单已处理，无需重复检查"
        }

    # 执行支付检查
    result = check_pending_payments(db)

    # 重新查询订单状态
    db.refresh(order)

    return {
        "order_no": order_no,
        "status": order.status.value,
        "paid_at": order.paid_at,
        "confirmed_at": order.confirmed_at,
        "message": "支付检查完成"
    }


# ==================== 管理员接口 ====================

@router.get("/admin/orders", response_model=List[RechargeOrderResponse])
async def list_all_recharge_orders(
    status_filter: Optional[str] = None,
    user_id: Optional[int] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取所有充值订单（管理员）

    status_filter: 可选状态筛选
    user_id: 可选用户筛选
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问此接口"
        )

    query = db.query(RechargeOrder)

    if status_filter:
        try:
            status_enum = RechargeOrderStatus(status_filter)
            query = query.filter(RechargeOrder.status == status_enum)
        except ValueError:
            pass

    if user_id:
        query = query.filter(RechargeOrder.user_id == user_id)

    orders = query.order_by(RechargeOrder.created_at.desc()).all()

    return [
        RechargeOrderResponse(
            id=o.id,
            order_no=o.order_no,
            user_id=o.user_id,
            amount=float(o.amount),
            status=o.status.value,
            alipay_trade_no=o.alipay_trade_no,
            paid_at=o.paid_at,
            confirmed_at=o.confirmed_at,
            expired_at=o.expired_at,
            created_at=o.created_at
        )
        for o in orders
    ]


@router.post("/admin/check-payments")
async def admin_check_payments(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    管理员手动触发支付检查

    遍历所有待支付订单，查询支付宝交易状态
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问此接口"
        )

    result = check_pending_payments(db)
    return result


@router.post("/admin/orders/{order_no}/distribute")
async def admin_distribute_order(
    order_no: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    管理员手动触发分账

    对于已支付但未分账的订单，执行分账操作
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问此接口"
        )

    order = db.query(RechargeOrder).filter(
        RechargeOrder.order_no == order_no
    ).first()

    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="订单不存在"
        )

    if order.status != RechargeOrderStatus.PAID:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"订单状态为 {order.status.value}，无法分账"
        )

    try:
        transfers = distribute_amount(order, db)
        db.refresh(order)

        return {
            "order_no": order_no,
            "status": order.status.value,
            "transfers_count": len(transfers),
            "message": "分账完成"
        }
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/admin/transfers", response_model=List[TransferRecordResponse])
async def list_transfers(
    order_id: Optional[int] = None,
    status_filter: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取转账记录列表（管理员）

    order_id: 可选订单ID筛选
    status_filter: 可选状态筛选
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问此接口"
        )

    query = db.query(TransferRecord)

    if order_id:
        query = query.filter(TransferRecord.recharge_order_id == order_id)

    if status_filter:
        try:
            status_enum = TransferStatus(status_filter)
            query = query.filter(TransferRecord.status == status_enum)
        except ValueError:
            pass

    transfers = query.order_by(TransferRecord.created_at.desc()).all()

    return [
        TransferRecordResponse(
            id=t.id,
            recharge_order_id=t.recharge_order_id,
            user_id=t.user_id,
            amount=float(t.amount),
            role=t.role,
            alipay_account=t.alipay_account,
            alipay_order_id=t.alipay_order_id,
            status=t.status.value,
            fail_reason=t.fail_reason,
            transferred_at=t.transferred_at,
            created_at=t.created_at
        )
        for t in transfers
    ]


@router.get("/admin/alipay-config", response_model=AlipayConfigResponse)
async def get_alipay_config_info(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取支付宝配置信息（仅返回公开信息）

    用于前端展示收款码和费率配置
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问此接口"
        )

    config = get_alipay_config(db)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="支付宝配置未设置"
        )

    return AlipayConfigResponse(
        id=config.id,
        name=config.name,
        qrcode_url=config.qrcode_url,
        platform_fee_rate=float(config.platform_fee_rate),
        agent_l1_rate=float(config.agent_l1_rate),
        agent_l2_rate=float(config.agent_l2_rate),
        user_rate=float(config.user_rate)
    )


@router.get("/wallet")
async def get_wallet_info(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取当前用户钱包和支付宝信息"""
    info = get_wallet_with_alipay(current_user.id, db)
    if not info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="用户不存在"
        )
    return info


@router.post("/admin/orders/{order_no}/manual-confirm")
async def admin_manual_confirm_payment(
    order_no: str,
    alipay_trade_no: str = Body(..., embed=True, description="支付宝交易号"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    管理员手动确认支付

    用于个人收款码转账场景：
    1. 用户扫描收款码转账，在备注中填写订单号
    2. 管理员在支付宝账单中看到对应的转账记录
    3. 管理员复制支付宝交易号，使用此接口确认支付
    4. 系统自动执行分账
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问此接口"
        )

    try:
        result = manually_confirm_payment(order_no, alipay_trade_no, db)
        return result
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"确认失败: {str(e)}"
        )


@router.get("/admin/pending-orders")
async def get_pending_orders(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取待支付订单列表（管理员）

    用于管理员查看哪些订单需要手动确认支付
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="仅管理员可访问此接口"
        )

    pending_orders = db.query(RechargeOrder).filter(
        RechargeOrder.status == RechargeOrderStatus.PENDING,
        RechargeOrder.expired_at > datetime.now()
    ).order_by(RechargeOrder.created_at.desc()).all()

    return {
        "count": len(pending_orders),
        "orders": [
            {
                "id": o.id,
                "order_no": o.order_no,
                "user_id": o.user_id,
                "amount": float(o.amount),
                "created_at": o.created_at.isoformat() if o.created_at else None,
                "expired_at": o.expired_at.isoformat() if o.expired_at else None
            }
            for o in pending_orders
        ]
    }
