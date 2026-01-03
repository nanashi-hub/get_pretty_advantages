from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.auth import get_current_user
from app.database import get_db
from app.models import User, UserRole, WalletAccount, WalletLedger, WithdrawRequest
from app.schemas import WithdrawRequestCreate, WithdrawRequestReject, WithdrawRequestResponse

router = APIRouter(prefix="/api", tags=["提现"])


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="需要管理员权限")
    return current_user


def _get_or_create_wallet_locked(db: Session, user_id: int) -> WalletAccount:
    wallet = db.query(WalletAccount).filter(WalletAccount.user_id == int(user_id)).with_for_update().first()
    if wallet:
        return wallet
    wallet = WalletAccount(user_id=int(user_id), available_coins=0, locked_coins=0)
    db.add(wallet)
    db.flush()
    return wallet


@router.post("/withdraw-requests", response_model=WithdrawRequestResponse, status_code=status.HTTP_201_CREATED)
async def create_withdraw_request(
    data: WithdrawRequestCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """用户发起提现申请（最简版：申请时直接扣减 available）"""
    amount = int(data.amount_coins)
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount_coins 必须大于 0")

    now = datetime.now()

    with db.begin():
        wallet = _get_or_create_wallet_locked(db, current_user.id)
        available = int(wallet.available_coins or 0)
        if available < amount:
            raise HTTPException(status_code=400, detail="可提现余额不足")

        wallet.available_coins = available - amount

        req = WithdrawRequest(
            user_id=current_user.id,
            amount_coins=amount,
            method=data.method,
            account_info=data.account_info,
            status=0,
            requested_at=now,
        )
        db.add(req)
        db.flush()

        db.add(
            WalletLedger(
                user_id=current_user.id,
                period_id=None,
                entry_type="WITHDRAW_APPLY",
                delta_available_coins=-amount,
                delta_locked_coins=0,
                ref_source_user_id=None,
                remark=f"withdraw apply #{req.withdraw_id}",
            )
        )

    db.refresh(req)
    return req


@router.get("/withdraw-requests/my", response_model=List[WithdrawRequestResponse])
async def list_my_withdraw_requests(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """我的提现记录"""
    return (
        db.query(WithdrawRequest)
        .filter(WithdrawRequest.user_id == current_user.id)
        .order_by(WithdrawRequest.withdraw_id.desc())
        .limit(int(limit))
        .all()
    )


@router.post("/withdraw-requests/{withdraw_id}/cancel", response_model=WithdrawRequestResponse)
async def cancel_withdraw_request(
    withdraw_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """用户取消提现（仅 PENDING）并回滚余额"""
    now = datetime.now()

    with db.begin():
        req = (
            db.query(WithdrawRequest)
            .filter(WithdrawRequest.withdraw_id == int(withdraw_id))
            .with_for_update()
            .first()
        )
        if not req or int(req.user_id) != int(current_user.id):
            raise HTTPException(status_code=404, detail="提现申请不存在")
        if int(req.status or 0) != 0:
            raise HTTPException(status_code=400, detail="仅待审核的提现可以取消")

        wallet = _get_or_create_wallet_locked(db, current_user.id)
        wallet.available_coins = int(wallet.available_coins or 0) + int(req.amount_coins or 0)

        req.status = 4
        req.processed_at = now
        req.processed_by = None
        req.reject_reason = None

        db.add(
            WalletLedger(
                user_id=current_user.id,
                period_id=None,
                entry_type="WITHDRAW_REFUND",
                delta_available_coins=int(req.amount_coins or 0),
                delta_locked_coins=0,
                ref_source_user_id=None,
                remark=f"withdraw canceled #{req.withdraw_id}",
            )
        )

    db.refresh(req)
    return req


@router.get("/withdraw-requests", response_model=List[WithdrawRequestResponse])
async def list_withdraw_requests_admin(
    status_filter: Optional[int] = Query(None, alias="status"),
    user_id: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """提现列表（管理员）"""
    query = db.query(WithdrawRequest)
    if status_filter is not None:
        query = query.filter(WithdrawRequest.status == int(status_filter))
    if user_id is not None:
        query = query.filter(WithdrawRequest.user_id == int(user_id))
    return query.order_by(WithdrawRequest.withdraw_id.desc()).limit(int(limit)).all()


@router.post("/withdraw-requests/{withdraw_id}/approve", response_model=WithdrawRequestResponse)
async def approve_withdraw_request(
    withdraw_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员审核通过（可选环节）"""
    now = datetime.now()

    with db.begin():
        req = (
            db.query(WithdrawRequest)
            .filter(WithdrawRequest.withdraw_id == int(withdraw_id))
            .with_for_update()
            .first()
        )
        if not req:
            raise HTTPException(status_code=404, detail="提现申请不存在")
        if int(req.status or 0) != 0:
            raise HTTPException(status_code=400, detail="仅待审核的提现可以通过")

        req.status = 1
        req.processed_at = now
        req.processed_by = current_user.id
        req.reject_reason = None

    db.refresh(req)
    return req


@router.post("/withdraw-requests/{withdraw_id}/pay", response_model=WithdrawRequestResponse)
async def pay_withdraw_request(
    withdraw_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员标记已打款（最简版：不做额外余额变动，仅审计）"""
    now = datetime.now()

    with db.begin():
        req = (
            db.query(WithdrawRequest)
            .filter(WithdrawRequest.withdraw_id == int(withdraw_id))
            .with_for_update()
            .first()
        )
        if not req:
            raise HTTPException(status_code=404, detail="提现申请不存在")
        if int(req.status or 0) in (2, 3, 4):
            raise HTTPException(status_code=400, detail="该提现已结束，无法重复打款")

        req.status = 2
        req.processed_at = now
        req.processed_by = current_user.id
        req.reject_reason = None

        db.add(
            WalletLedger(
                user_id=int(req.user_id),
                period_id=None,
                entry_type="WITHDRAW_PAID",
                delta_available_coins=0,
                delta_locked_coins=0,
                ref_source_user_id=None,
                remark=f"withdraw paid #{req.withdraw_id}",
            )
        )

    db.refresh(req)
    return req


@router.post("/withdraw-requests/{withdraw_id}/reject", response_model=WithdrawRequestResponse)
async def reject_withdraw_request(
    withdraw_id: int,
    data: WithdrawRequestReject,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """管理员驳回提现并回滚余额"""
    now = datetime.now()

    with db.begin():
        req = (
            db.query(WithdrawRequest)
            .filter(WithdrawRequest.withdraw_id == int(withdraw_id))
            .with_for_update()
            .first()
        )
        if not req:
            raise HTTPException(status_code=404, detail="提现申请不存在")
        if int(req.status or 0) in (2, 3, 4):
            raise HTTPException(status_code=400, detail="该提现已结束，无法驳回")

        wallet = _get_or_create_wallet_locked(db, int(req.user_id))
        wallet.available_coins = int(wallet.available_coins or 0) + int(req.amount_coins or 0)

        req.status = 3
        req.processed_at = now
        req.processed_by = current_user.id
        req.reject_reason = data.reject_reason

        db.add(
            WalletLedger(
                user_id=int(req.user_id),
                period_id=None,
                entry_type="WITHDRAW_REFUND",
                delta_available_coins=int(req.amount_coins or 0),
                delta_locked_coins=0,
                ref_source_user_id=None,
                remark=f"withdraw rejected #{req.withdraw_id}",
            )
        )

    db.refresh(req)
    return req
