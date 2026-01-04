from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.auth import get_current_user
from app.database import get_db
from app.models import (
    SettlementCommission,
    SettlementPeriod,
    SettlementUserPayable,
    User,
    WalletAccount,
    WalletLedger,
)
from app.schemas import (
    SettlementPeriodResponse,
    SettlementUserPayableResponse,
    WalletAccountResponse,
    WalletLedgerEntryResponse,
    WalletSummaryResponse,
)

router = APIRouter(prefix="/api", tags=["钱包"])

DEFAULT_COIN_RATE = 10000


def _get_current_period(db: Session) -> Optional[SettlementPeriod]:
    active_period = db.query(SettlementPeriod).filter(SettlementPeriod.is_active == 1).first()
    if active_period:
        return active_period

    return (
        db.query(SettlementPeriod)
        .filter(SettlementPeriod.status.in_([0, 1]))
        .order_by(SettlementPeriod.period_id.desc())
        .first()
    )


def _get_coin_rate(period: Optional[SettlementPeriod]) -> int:
    if period and int(getattr(period, "coin_rate", 0) or 0) > 0:
        return int(period.coin_rate)
    return DEFAULT_COIN_RATE


def _get_or_create_wallet(db: Session, user_id: int) -> WalletAccount:
    wallet = db.query(WalletAccount).filter(WalletAccount.user_id == user_id).first()
    if wallet:
        return wallet
    wallet = WalletAccount(user_id=user_id, available_coins=0, locked_coins=0)
    db.add(wallet)
    db.commit()
    db.refresh(wallet)
    return wallet


@router.get("/wallet", response_model=WalletAccountResponse)
async def get_wallet(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """获取我的钱包账户（coins）"""
    return _get_or_create_wallet(db, current_user.id)


@router.get("/wallet/summary", response_model=WalletSummaryResponse)
async def get_wallet_summary(
    period_id: Optional[int] = Query(None, description="为空则取当前结算期"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """钱包页汇总数据：可用/锁定、本期应缴、下级待缴汇总、分成预估"""
    wallet = _get_or_create_wallet(db, current_user.id)

    if period_id is None:
        period = _get_current_period(db)
    else:
        period = db.query(SettlementPeriod).filter(SettlementPeriod.period_id == int(period_id)).first()

    coin_rate = _get_coin_rate(period)

    my_payable = None
    my_remaining_due_coins = 0
    l1_cnt = 0
    l1_sum_due = 0
    l2_cnt = 0
    l2_sum_due = 0
    commission_expected = 0
    commission_funded_locked = 0
    commission_unfunded = 0

    if period:
        my_payable = db.query(SettlementUserPayable).filter(
            SettlementUserPayable.period_id == int(period.period_id),
            SettlementUserPayable.user_id == current_user.id,
        ).first()
        if my_payable:
            due = int(my_payable.amount_due_coins or 0)
            paid = int(my_payable.amount_paid_coins or 0)
            my_remaining_due_coins = max(0, due - paid)

        # 下级待缴汇总（以 snapshot 为准）
        l1_row = db.execute(
            text(
                """
                SELECT
                  COUNT(*) AS cnt,
                  COALESCE(SUM(p.amount_due_coins - p.amount_paid_coins), 0) AS sum_due
                FROM settlement_user_payable p
                JOIN settlement_referral_snapshot s
                  ON s.period_id = p.period_id AND s.user_id = p.user_id
                WHERE p.period_id = :period_id
                  AND s.inviter_level1 = :me
                  AND p.status <> 2
                """
            ),
            {"period_id": int(period.period_id), "me": int(current_user.id)},
        ).mappings().first()
        if l1_row:
            l1_cnt = int(l1_row.get("cnt") or 0)
            l1_sum_due = int(l1_row.get("sum_due") or 0)

        l2_row = db.execute(
            text(
                """
                SELECT
                  COUNT(*) AS cnt,
                  COALESCE(SUM(p.amount_due_coins - p.amount_paid_coins), 0) AS sum_due
                FROM settlement_user_payable p
                JOIN settlement_referral_snapshot s
                  ON s.period_id = p.period_id AND s.user_id = p.user_id
                WHERE p.period_id = :period_id
                  AND s.inviter_level2 = :me
                  AND p.status <> 2
                """
            ),
            {"period_id": int(period.period_id), "me": int(current_user.id)},
        ).mappings().first()
        if l2_row:
            l2_cnt = int(l2_row.get("cnt") or 0)
            l2_sum_due = int(l2_row.get("sum_due") or 0)

        # 分成预估（本期：含未资金化）
        commission_expected = int(
            db.query(func.coalesce(func.sum(SettlementCommission.amount_coins), 0))
            .filter(
                SettlementCommission.period_id == int(period.period_id),
                SettlementCommission.beneficiary_user_id == int(current_user.id),
            )
            .scalar()
            or 0
        )
        commission_funded_locked = int(
            db.query(func.coalesce(func.sum(SettlementCommission.amount_coins), 0))
            .filter(
                SettlementCommission.period_id == int(period.period_id),
                SettlementCommission.beneficiary_user_id == int(current_user.id),
                SettlementCommission.funding_status == 1,
                SettlementCommission.is_unlocked == 0,
            )
            .scalar()
            or 0
        )
        commission_unfunded = int(
            db.query(func.coalesce(func.sum(SettlementCommission.amount_coins), 0))
            .filter(
                SettlementCommission.period_id == int(period.period_id),
                SettlementCommission.beneficiary_user_id == int(current_user.id),
                SettlementCommission.funding_status == 0,
            )
            .scalar()
            or 0
        )

    return WalletSummaryResponse(
        coin_rate=coin_rate,
        wallet=WalletAccountResponse.model_validate(wallet),
        period=SettlementPeriodResponse.model_validate(period) if period else None,
        my_payable=SettlementUserPayableResponse.model_validate(my_payable) if my_payable else None,
        my_remaining_due_coins=my_remaining_due_coins,
        l1_due={"cnt": l1_cnt, "sum_due_coins": l1_sum_due},
        l2_due={"cnt": l2_cnt, "sum_due_coins": l2_sum_due},
        commission_expected_coins=commission_expected,
        commission_funded_locked_coins=commission_funded_locked,
        commission_unfunded_coins=commission_unfunded,
    )


@router.get("/wallet/ledger", response_model=List[WalletLedgerEntryResponse])
async def list_wallet_ledger(
    limit: int = Query(100, ge=1, le=500),
    period_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """账本流水（最近 N 条）"""
    query = db.query(WalletLedger).filter(WalletLedger.user_id == current_user.id)
    if period_id is not None:
        query = query.filter(WalletLedger.period_id == int(period_id))
    return query.order_by(WalletLedger.ledger_id.desc()).limit(int(limit)).all()
