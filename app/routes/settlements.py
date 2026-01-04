from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.auth import get_current_user
from app.database import get_db
from app.models import (
    SettlementCommission,
    SettlementPayment,
    SettlementPeriod,
    SettlementReferralSnapshot,
    SettlementUserIncome,
    SettlementUserPayable,
    User,
    UserRole,
    WalletLedger,
)
from app.schemas import (
    SettlementMeResponse,
    SettlementPaymentCreate,
    SettlementPaymentReject,
    SettlementPaymentResponse,
    SettlementPeriodCreate,
    SettlementPeriodResponse,
    SettlementUserIncomeResponse,
    SettlementUserPayableResponse,
)
from app.services.alipay_service import get_alipay_config
from app.services.settlement_unlock import unlock_commissions_for_beneficiary, unlock_commissions_for_period

router = APIRouter(prefix="/api", tags=["结算"])


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """要求管理员权限"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="需要管理员权限")
    return current_user


def _validate_period_create(data: SettlementPeriodCreate) -> None:
    if data.period_start > data.period_end:
        raise HTTPException(status_code=400, detail="period_start 不能晚于 period_end")
    if data.pay_start > data.pay_end:
        raise HTTPException(status_code=400, detail="pay_start 不能晚于 pay_end")

    if data.coin_rate <= 0:
        raise HTTPException(status_code=400, detail="coin_rate 必须大于 0")

    if not (0 <= data.host_bps <= 10000 and 0 <= data.collect_bps <= 10000):
        raise HTTPException(status_code=400, detail="host_bps/collect_bps 必须在 0~10000 之间")
    if data.host_bps + data.collect_bps != 10000:
        raise HTTPException(status_code=400, detail="host_bps + collect_bps 必须等于 10000")

    if not (0 <= data.l1_bps <= 10000 and 0 <= data.l2_bps <= 10000):
        raise HTTPException(status_code=400, detail="l1_bps/l2_bps 必须在 0~10000 之间")
    if data.l1_bps + data.l2_bps > data.collect_bps:
        raise HTTPException(status_code=400, detail="l1_bps + l2_bps 不能大于 collect_bps")

    if data.status not in (0, 1, 2):
        raise HTTPException(status_code=400, detail="status 仅支持 0/1/2")


def _get_period_or_404(db: Session, period_id: int) -> SettlementPeriod:
    period = db.query(SettlementPeriod).filter(SettlementPeriod.period_id == period_id).first()
    if not period:
        raise HTTPException(status_code=404, detail="结算期不存在")
    return period


def _get_current_period(db: Session, user_id: Optional[int] = None) -> Optional[SettlementPeriod]:
    """
    获取当前结算期

    策���（按优先级）：
    1. 优先返回 is_active=1 的结算期（管理员设置的当前生效期）
    2. 如果没有生效期，且提供了 user_id，返回该用户有未缴清记录的结算期
    3. 如果都没有，返回最新的 OPEN/PENDING 结算期
    """
    from app.models import SettlementUserPayable

    # 1. 优先查找管理员设置的当前生效期（is_active=1）
    active_period = (
        db.query(SettlementPeriod)
        .filter(SettlementPeriod.is_active == 1)
        .first()
    )
    if active_period:
        return active_period

    # 2. 如果提供了 user_id，查找该用户有未缴清记录的结算期
    if user_id is not None:
        unpaid_period = (
            db.query(SettlementPeriod)
            .join(SettlementUserPayable, SettlementPeriod.period_id == SettlementUserPayable.period_id)
            .filter(
                SettlementUserPayable.user_id == user_id,
                SettlementUserPayable.status != 2,  # 未缴清（不是 PAID 状态）
                SettlementPeriod.status.in_([0, 1])  # 结算期状态为 PENDING 或 OPEN
            )
            .order_by(SettlementPeriod.period_id.desc())
            .first()
        )
        if unpaid_period:
            return unpaid_period

    # 3. 返回最新的结算期
    return (
        db.query(SettlementPeriod)
        .filter(SettlementPeriod.status.in_([0, 1]))
        .order_by(SettlementPeriod.period_id.desc())
        .first()
    )


def _assert_in_pay_window(period: SettlementPeriod, today: date) -> None:
    if today < period.pay_start or today > period.pay_end:
        raise HTTPException(
            status_code=400,
            detail=f"当前不在缴费窗口（{period.pay_start}~{period.pay_end}）",
        )


@router.get("/settlement/me", response_model=SettlementMeResponse)
async def get_my_settlement_center(
    period_id: Optional[int] = Query(None, description="为空则取当前结算期"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """结算中心（用户视角）"""
    alipay_config = get_alipay_config(db)
    alipay_qrcode_url = alipay_config.qrcode_url if alipay_config else None

    if period_id is None:
        period = _get_current_period(db, user_id=current_user.id)
        if not period:
            return SettlementMeResponse(
                period=None,
                income=None,
                payable=None,
                payments=[],
                alipay_qrcode_url=alipay_qrcode_url,
            )
        period_id = int(period.period_id)
    else:
        period = _get_period_or_404(db, int(period_id))

    income = db.query(SettlementUserIncome).filter(
        SettlementUserIncome.period_id == int(period_id),
        SettlementUserIncome.user_id == current_user.id,
    ).first()
    payable = db.query(SettlementUserPayable).filter(
        SettlementUserPayable.period_id == int(period_id),
        SettlementUserPayable.user_id == current_user.id,
    ).first()
    payments = db.query(SettlementPayment).filter(
        SettlementPayment.period_id == int(period_id),
        SettlementPayment.payer_user_id == current_user.id,
    ).order_by(SettlementPayment.payment_id.desc()).all()

    return SettlementMeResponse(
        period=SettlementPeriodResponse.model_validate(period) if period else None,
        income=SettlementUserIncomeResponse.model_validate(income) if income else None,
        payable=SettlementUserPayableResponse.model_validate(payable) if payable else None,
        payments=[SettlementPaymentResponse.model_validate(p) for p in payments],
        alipay_qrcode_url=alipay_qrcode_url,
    )


@router.get("/settlement-periods/current", response_model=Optional[SettlementPeriodResponse])
async def get_current_settlement_period(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """获取当前结算期（优先 PAYING，其次最新的 OPEN/PAYING，其它为空）"""
    period = (
        db.query(SettlementPeriod)
        .filter(SettlementPeriod.status.in_([0, 1]))
        .order_by(SettlementPeriod.period_id.desc())
        .first()
    )
    if not period:
        return None

    # 手动添加 period_label 字段
    return SettlementPeriodResponse(
        period_id=period.period_id,
        period_label=period.period_label,
        period_start=period.period_start,
        period_end=period.period_end,
        pay_start=period.pay_start,
        pay_end=period.pay_end,
        coin_rate=period.coin_rate,
        host_bps=period.host_bps,
        l1_bps=period.l1_bps,
        l2_bps=period.l2_bps,
        collect_bps=period.collect_bps,
        status=period.status,
        is_active=period.is_active,
        created_at=period.created_at,
        updated_at=period.updated_at,
    )


@router.get("/settlement-periods", response_model=List[SettlementPeriodResponse])
async def list_settlement_periods(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """结算期列表（管理员）"""
    periods = db.query(SettlementPeriod).order_by(SettlementPeriod.period_id.desc()).all()

    # 手动添加 period_label 字段（hybrid_property 在 Pydantic v1 中可能无法自动序列化）
    result = []
    for p in periods:
        p_dict = {
            "period_id": p.period_id,
            "period_label": p.period_label,
            "period_start": p.period_start,
            "period_end": p.period_end,
            "pay_start": p.pay_start,
            "pay_end": p.pay_end,
            "coin_rate": p.coin_rate,
            "host_bps": p.host_bps,
            "l1_bps": p.l1_bps,
            "l2_bps": p.l2_bps,
            "collect_bps": p.collect_bps,
            "status": p.status,
            "is_active": p.is_active,
            "created_at": p.created_at,
            "updated_at": p.updated_at,
        }
        result.append(SettlementPeriodResponse(**p_dict))
    return result


@router.post("/settlement-periods", response_model=SettlementPeriodResponse)
async def create_settlement_period(
    data: SettlementPeriodCreate,
    response: Response,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """创建结算期（管理员）"""
    _validate_period_create(data)

    existing = db.query(SettlementPeriod).filter(
        SettlementPeriod.period_start == data.period_start,
        SettlementPeriod.period_end == data.period_end,
    ).first()
    if existing:
        response.status_code = status.HTTP_200_OK
        return SettlementPeriodResponse(
            period_id=existing.period_id,
            period_label=existing.period_label,
            period_start=existing.period_start,
            period_end=existing.period_end,
            pay_start=existing.pay_start,
            pay_end=existing.pay_end,
            coin_rate=existing.coin_rate,
            host_bps=existing.host_bps,
            l1_bps=existing.l1_bps,
            l2_bps=existing.l2_bps,
            collect_bps=existing.collect_bps,
            status=existing.status,
            is_active=existing.is_active,
            created_at=existing.created_at,
            updated_at=existing.updated_at,
        )

    period = SettlementPeriod(**data.model_dump())
    db.add(period)
    db.commit()
    db.refresh(period)
    response.status_code = status.HTTP_201_CREATED
    return SettlementPeriodResponse(
        period_id=period.period_id,
        period_label=period.period_label,
        period_start=period.period_start,
        period_end=period.period_end,
        pay_start=period.pay_start,
        pay_end=period.pay_end,
        coin_rate=period.coin_rate,
        host_bps=period.host_bps,
        l1_bps=period.l1_bps,
        l2_bps=period.l2_bps,
        collect_bps=period.collect_bps,
        status=period.status,
        is_active=period.is_active,
        created_at=period.created_at,
        updated_at=period.updated_at,
    )


@router.post("/settlement-periods/{period_id}/generate")
async def generate_settlement_for_period(
    period_id: int,
    regenerate: bool = Query(False, description="是否重跑（会清空该 period_id 的快照/汇总/应缴数据）"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """
    生成本期结算（阶段1 MVP）
    - 生成关系快照 settlement_referral_snapshot（从 user_referrals 全量拷贝）
    - 聚合 earning_records 写入 settlement_user_income（只统计 period_start~period_end）
    - 基于 settlement_user_income 写入 settlement_user_payable（amount_due_coins = self_payable_coins）
    """
    _get_period_or_404(db, period_id)

    # 先提交可能存在的挂起事务
    db.commit()

    has_snapshot = db.query(SettlementReferralSnapshot).filter(SettlementReferralSnapshot.period_id == period_id).first()
    has_income = db.query(SettlementUserIncome).filter(SettlementUserIncome.period_id == period_id).first()
    has_payable = db.query(SettlementUserPayable).filter(SettlementUserPayable.period_id == period_id).first()
    has_commissions = db.query(SettlementCommission).filter(SettlementCommission.period_id == period_id).first()

    if not regenerate and (has_snapshot or has_income or has_payable or has_commissions):
        raise HTTPException(status_code=400, detail="该结算期已生成过，如需重跑请传 regenerate=true")

    if regenerate:
        any_payment = db.query(SettlementPayment).filter(SettlementPayment.period_id == period_id).first()
        if any_payment:
            raise HTTPException(status_code=400, detail="该结算期已存在缴费记录，禁止重跑")

    try:
        # 关系快照：冻结本期 +1/+2 关系
        db.execute(
            text(
                """
                INSERT INTO settlement_referral_snapshot(period_id, user_id, inviter_level1, inviter_level2)
                SELECT :period_id, r.user_id, r.inviter_level1, r.inviter_level2
                FROM user_referrals r
                """
            ),
            {"period_id": period_id},
        )

        # earning_records -> settlement_user_income（按期聚合并按 bps 拆分）
        db.execute(
            text(
                """
                INSERT INTO settlement_user_income
                (period_id, user_id, gross_coins, self_keep_coins, self_payable_coins,
                 l1_user_id, l2_user_id, l1_commission_coins, l2_commission_coins, platform_retain_coins)
                SELECT
                  p.period_id,
                  er.user_id,
                  SUM(er.coins_total) AS gross_coins,
                  (SUM(er.coins_total) * p.host_bps)    DIV 10000 AS self_keep_coins,
                  (SUM(er.coins_total) * p.collect_bps) DIV 10000 AS self_payable_coins,
                  s.inviter_level1 AS l1_user_id,
                  s.inviter_level2 AS l2_user_id,
                  CASE WHEN s.inviter_level1 IS NULL THEN 0 ELSE (SUM(er.coins_total) * p.l1_bps) DIV 10000 END AS l1_commission_coins,
                  CASE WHEN s.inviter_level2 IS NULL THEN 0 ELSE (SUM(er.coins_total) * p.l2_bps) DIV 10000 END AS l2_commission_coins,
                  (
                    (SUM(er.coins_total) * p.collect_bps) DIV 10000
                    - CASE WHEN s.inviter_level1 IS NULL THEN 0 ELSE (SUM(er.coins_total) * p.l1_bps) DIV 10000 END
                    - CASE WHEN s.inviter_level2 IS NULL THEN 0 ELSE (SUM(er.coins_total) * p.l2_bps) DIV 10000 END
                  ) AS platform_retain_coins
                FROM settlement_periods p
                JOIN earning_records er
                  ON er.stat_date BETWEEN p.period_start AND p.period_end
                LEFT JOIN settlement_referral_snapshot s
                  ON s.period_id = p.period_id AND s.user_id = er.user_id
                WHERE p.period_id = :period_id
                  AND er.user_id IS NOT NULL
                GROUP BY p.period_id, er.user_id, s.inviter_level1, s.inviter_level2
                """
            ),
            {"period_id": period_id},
        )

        # settlement_user_income -> settlement_commissions（生成分成明细，默认 funding_status=0）
        db.execute(
            text(
                """
                INSERT INTO settlement_commissions(period_id, source_user_id, beneficiary_user_id, level, amount_coins)
                SELECT period_id, user_id, l1_user_id, 1, l1_commission_coins
                FROM settlement_user_income
                WHERE period_id = :period_id
                  AND l1_user_id IS NOT NULL
                  AND l1_commission_coins > 0
                """
            ),
            {"period_id": period_id},
        )
        db.execute(
            text(
                """
                INSERT INTO settlement_commissions(period_id, source_user_id, beneficiary_user_id, level, amount_coins)
                SELECT period_id, user_id, l2_user_id, 2, l2_commission_coins
                FROM settlement_user_income
                WHERE period_id = :period_id
                  AND l2_user_id IS NOT NULL
                  AND l2_commission_coins > 0
                """
            ),
            {"period_id": period_id},
        )

        # settlement_user_income -> settlement_user_payable（应缴=40%）
        db.execute(
            text(
                """
                INSERT INTO settlement_user_payable(period_id, user_id, amount_due_coins, amount_paid_coins, status)
                SELECT period_id, user_id, self_payable_coins, 0, 0
                FROM settlement_user_income
                WHERE period_id = :period_id
                """
            ),
            {"period_id": period_id},
        )

        # 生成后进入 PAYING
        db.execute(
            text("UPDATE settlement_periods SET status = 1 WHERE period_id = :period_id"),
            {"period_id": period_id},
        )

        db.commit()

        return {"message": "生成成功", "period_id": period_id}
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"生成失败: {exc}")


@router.post("/settlement-periods/{period_id}/generate-commissions")
async def generate_commissions_for_period(
    period_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """为指定结算期补生成 settlement_commissions（不删除、不重跑，INSERT IGNORE 幂等）"""
    _get_period_or_404(db, int(period_id))
    db.commit()

    try:
        db.execute(
            text(
                """
                INSERT IGNORE INTO settlement_commissions(period_id, source_user_id, beneficiary_user_id, level, amount_coins)
                SELECT period_id, user_id, l1_user_id, 1, l1_commission_coins
                FROM settlement_user_income
                WHERE period_id = :period_id
                  AND l1_user_id IS NOT NULL
                  AND l1_commission_coins > 0
                """
            ),
            {"period_id": int(period_id)},
        )
        db.execute(
            text(
                """
                INSERT IGNORE INTO settlement_commissions(period_id, source_user_id, beneficiary_user_id, level, amount_coins)
                SELECT period_id, user_id, l2_user_id, 2, l2_commission_coins
                FROM settlement_user_income
                WHERE period_id = :period_id
                  AND l2_user_id IS NOT NULL
                  AND l2_commission_coins > 0
                """
            ),
            {"period_id": int(period_id)},
        )
        db.commit()
        return {"message": "commission 已补生成", "period_id": int(period_id)}
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"补生成失败: {exc}")


@router.post("/settlement-periods/{period_id}/unlock-commissions")
async def unlock_commissions(
    period_id: int,
    beneficiary_user_id: Optional[int] = Query(None, description="可选：仅解锁指定受益人 user_id"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """批量解锁分成（满足条件：commission FUNDED + beneficiary 已缴清）"""
    _get_period_or_404(db, int(period_id))
    db.commit()

    try:
        if beneficiary_user_id is not None:
            unlocked = unlock_commissions_for_beneficiary(
                db,
                int(period_id),
                int(beneficiary_user_id),
            )
            db.commit()
            return {
                "message": "ok",
                "period_id": int(period_id),
                "beneficiary_user_id": int(beneficiary_user_id),
                "unlocked_coins": int(unlocked),
            }

        result = unlock_commissions_for_period(db, int(period_id))
        db.commit()
        return {"message": "ok", "period_id": int(period_id), **result}
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail=str(exc))
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"解锁失败: {exc}")


@router.post("/settlement-payments", response_model=SettlementPaymentResponse, status_code=status.HTTP_201_CREATED)
async def create_settlement_payment(
    data: SettlementPaymentCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """提交缴费（用户）"""
    period_id = data.period_id
    if period_id is None:
        period = _get_current_period(db)
        if not period:
            raise HTTPException(status_code=400, detail="当前无可用结算期")
        period_id = int(period.period_id)
    else:
        period = _get_period_or_404(db, int(period_id))

    today = date.today()
    _assert_in_pay_window(period, today)

    payable = db.query(SettlementUserPayable).filter(
        SettlementUserPayable.period_id == int(period_id),
        SettlementUserPayable.user_id == current_user.id,
    ).first()
    if not payable:
        raise HTTPException(status_code=404, detail="本期未生成应缴记录，无法提交缴费")

    remaining = int(payable.amount_due_coins or 0) - int(payable.amount_paid_coins or 0)
    if remaining <= 0:
        raise HTTPException(status_code=400, detail="本期已缴清或无需缴费")
    if data.amount_coins > remaining:
        raise HTTPException(status_code=400, detail=f"本次缴费金额不能超过剩余应缴（{remaining} coins）")

    payment = SettlementPayment(
        period_id=int(period_id),
        payer_user_id=current_user.id,
        amount_coins=int(data.amount_coins),
        method=data.method,
        proof_url=data.proof_url,
        status=0,
    )
    db.add(payment)
    db.commit()
    db.refresh(payment)
    return payment


@router.get("/settlement-payments/my", response_model=List[SettlementPaymentResponse])
async def list_my_settlement_payments(
    period_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """我的缴费记录（用户）"""
    query = db.query(SettlementPayment).filter(SettlementPayment.payer_user_id == current_user.id)
    if period_id is not None:
        query = query.filter(SettlementPayment.period_id == int(period_id))
    return query.order_by(SettlementPayment.payment_id.desc()).all()


@router.get("/settlement-payments", response_model=List[SettlementPaymentResponse])
async def list_settlement_payments(
    period_id: Optional[int] = Query(None),
    status_filter: Optional[int] = Query(None, alias="status"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """缴费记录列表（管理员）"""
    query = db.query(SettlementPayment)
    if period_id is not None:
        query = query.filter(SettlementPayment.period_id == int(period_id))
    if status_filter is not None:
        query = query.filter(SettlementPayment.status == int(status_filter))
    return query.order_by(SettlementPayment.payment_id.desc()).all()


@router.post("/settlement-payments/{payment_id}/confirm", response_model=SettlementPaymentResponse)
async def confirm_settlement_payment(
    payment_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """确认缴费（管理员）"""
    # MySQL DATETIME 默认不存微秒；后续 SQL 需要用 funded_at = :now 做精确匹配，因此统一截断到秒级
    now = datetime.now().replace(microsecond=0)

    try:
        payment = (
            db.query(SettlementPayment)
            .filter(SettlementPayment.payment_id == int(payment_id))
            .with_for_update()
            .first()
        )
        if not payment:
            raise HTTPException(status_code=404, detail="缴费记录不存在")
        if int(payment.status) != 0:
            raise HTTPException(status_code=400, detail="该记录不是待审核状态")

        payable = (
            db.query(SettlementUserPayable)
            .filter(
                SettlementUserPayable.period_id == int(payment.period_id),
                SettlementUserPayable.user_id == int(payment.payer_user_id),
            )
            .with_for_update()
            .first()
        )
        if not payable:
            raise HTTPException(status_code=404, detail="未找到对应的应缴记录")

        period = _get_period_or_404(db, int(payment.period_id))

        prev_payable_status = int(payable.status or 0)

        payment.status = 1
        payment.confirmed_at = now
        payment.confirmed_by = current_user.id
        payment.reject_reason = None

        due = int(payable.amount_due_coins or 0)
        paid_before = int(payable.amount_paid_coins or 0)
        paid_after = paid_before + int(payment.amount_coins or 0)
        payable.amount_paid_coins = paid_after

        if payable.first_paid_at is None:
            payable.first_paid_at = now

        if due <= 0:
            payable.status = 2
            if payable.paid_at is None:
                payable.paid_at = now
        elif paid_after >= due:
            payable.status = 2
            if payable.paid_at is None:
                payable.paid_at = now
        else:
            if date.today() > period.pay_end:
                payable.status = 3
            else:
                payable.status = 1 if paid_after > 0 else 0

        # 阶段2：首次缴清 -> 资金化分成并入账到上级钱包（locked）
        just_paid = prev_payable_status != 2 and int(payable.status or 0) == 2
        if just_paid:
            period_id = int(payment.period_id)
            source_user_id = int(payment.payer_user_id)

            # 将该来源用户本期的 commission 置 FUNDED（仅更新未资金化的行）
            db.execute(
                text(
                    """
                    UPDATE settlement_commissions
                    SET funding_status = 1,
                        funded_at = :now
                    WHERE period_id = :period_id
                      AND source_user_id = :source_user_id
                      AND funding_status = 0
                    """
                ),
                {"now": now, "period_id": period_id, "source_user_id": source_user_id},
            )

            # 写入账本（按 beneficiary 聚合；只处理本次刚资金化的行，避免重复入账）
            db.execute(
                text(
                    """
                    INSERT INTO wallet_ledger
                      (user_id, period_id, entry_type, delta_locked_coins, ref_source_user_id, remark)
                    SELECT
                      beneficiary_user_id,
                      :period_id,
                      'COMMISSION_LOCKED_IN',
                      SUM(amount_coins) AS sum_coins,
                      :source_user_id,
                      'downline paid'
                    FROM settlement_commissions
                    WHERE period_id = :period_id
                      AND source_user_id = :source_user_id
                      AND funding_status = 1
                      AND funded_at = :now
                    GROUP BY beneficiary_user_id
                    """
                ),
                {"now": now, "period_id": period_id, "source_user_id": source_user_id},
            )

            # 同步更新钱包账户 locked_coins（不存在则初始化）
            db.execute(
                text(
                    """
                    INSERT INTO wallet_accounts(user_id, available_coins, locked_coins)
                    SELECT
                      beneficiary_user_id,
                      0,
                      SUM(amount_coins) AS sum_coins
                    FROM settlement_commissions
                    WHERE period_id = :period_id
                      AND source_user_id = :source_user_id
                      AND funding_status = 1
                      AND funded_at = :now
                    GROUP BY beneficiary_user_id
                    ON DUPLICATE KEY UPDATE
                      locked_coins = locked_coins + VALUES(locked_coins)
                    """
                ),
                {"now": now, "period_id": period_id, "source_user_id": source_user_id},
            )

            # 阶段3：尝试即时解锁（满足"上级已缴清"的受益人，以及本次缴清的 payer 自己）
            beneficiary_rows = (
                db.execute(
                    text(
                        """
                        SELECT DISTINCT beneficiary_user_id
                        FROM settlement_commissions
                        WHERE period_id = :period_id
                          AND source_user_id = :source_user_id
                          AND funding_status = 1
                          AND funded_at = :now
                        """
                    ),
                    {"now": now, "period_id": period_id, "source_user_id": source_user_id},
                )
                .mappings()
                .all()
            )

            try:
                for r in beneficiary_rows:
                    bid = int(r.get("beneficiary_user_id") or 0)
                    if bid > 0:
                        unlock_commissions_for_beneficiary(db, period_id, bid, now=now)

                # payer 本人本期若存在已资金化但未解锁的分成，也在其"缴清"后立即解锁
                unlock_commissions_for_beneficiary(db, period_id, source_user_id, now=now)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc))

        db.commit()
    except Exception as exc:
        db.rollback()
        raise exc

    db.refresh(payment)
    return payment


@router.post("/settlement-payments/{payment_id}/reject", response_model=SettlementPaymentResponse)
async def reject_settlement_payment(
    payment_id: int,
    data: SettlementPaymentReject,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """驳回缴费（管理员）"""
    now = datetime.now()

    try:
        payment = (
            db.query(SettlementPayment)
            .filter(SettlementPayment.payment_id == int(payment_id))
            .with_for_update()
            .first()
        )
        if not payment:
            raise HTTPException(status_code=404, detail="缴费记录不存在")
        if int(payment.status) != 0:
            raise HTTPException(status_code=400, detail="该记录不是待审核状态")

        payment.status = 2
        payment.confirmed_at = now
        payment.confirmed_by = current_user.id
        payment.reject_reason = data.reject_reason

        db.commit()
    except Exception as exc:
        db.rollback()
        raise exc

    db.refresh(payment)
    return payment


# ==================== 结算期管理 API ====================

@router.post("/settlement-periods/{period_id}/activate")
async def activate_settlement_period(
    period_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """
    设置结算期为当前生效期（管理员）

    规则：
    - 将该结算期的 is_active 设为 1
    - 将其他所有结算期的 is_active 设为 0
    - 所有用户将统一使用这个生效期进行计算
    """
    period = _get_period_or_404(db, period_id)

    try:
        # 取消其他所有结算期的生效状态
        db.query(SettlementPeriod).filter(
            SettlementPeriod.period_id != int(period_id)
        ).update({"is_active": 0})

        # 设置当前结算期为生效期
        period.is_active = 1

        db.commit()
        return {"message": f"已设置 {period.period_start}~{period.period_end} 为当前生效期", "period_id": period_id}
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"设置生效期失败: {exc}")


@router.delete("/settlement-periods/{period_id}")
async def delete_settlement_period(
    period_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """
    删除结算期（管理员）

    限制：
    - 仅允许删除“无任何缴费记录”的结算期（避免影响已发生的资金与审计）
    - 若该期已产生钱包账本流水（wallet_ledger.period_id），也禁止删除
    - 删除时会同时清理关联的：关系快照/收益汇总/应缴/分成明细
    """
    period = _get_period_or_404(db, period_id)

    # 检查是否有缴费记录
    payment_count = db.query(SettlementPayment).filter(
        SettlementPayment.period_id == int(period_id)
    ).count()
    if payment_count > 0:
        raise HTTPException(
            status_code=400,
            detail=f"该结算期已有 {payment_count} 条缴费记录，无法删除"
        )

    # 检查是否已发生钱包入账/解锁流水（强审计表，不允许删除周期后造成断链）
    ledger_count = db.query(WalletLedger).filter(WalletLedger.period_id == int(period_id)).count()
    if ledger_count > 0:
        raise HTTPException(
            status_code=400,
            detail=f"该结算期已产生 {ledger_count} 条钱包账本流水，无法删除（请走关账/对账流程）"
        )

    try:
        # 若当前为生效期，先取消生效标记
        if int(period.is_active or 0) == 1:
            period.is_active = 0
            db.flush()

        # 先清理分成明细
        db.query(SettlementCommission).filter(SettlementCommission.period_id == int(period_id)).delete()

        # 删除关系快照
        db.query(SettlementReferralSnapshot).filter(
            SettlementReferralSnapshot.period_id == int(period_id)
        ).delete()

        # 删除用户收益汇总
        db.query(SettlementUserIncome).filter(
            SettlementUserIncome.period_id == int(period_id)
        ).delete()

        # 删除用户应缴记录
        db.query(SettlementUserPayable).filter(
            SettlementUserPayable.period_id == int(period_id)
        ).delete()

        # 删除结算期本身
        db.query(SettlementPeriod).filter(
            SettlementPeriod.period_id == int(period_id)
        ).delete()

        db.commit()
        return {"message": "结算期已删除", "period_id": period_id}
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"删除失败: {exc}")
