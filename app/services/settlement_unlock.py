from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


def unlock_commissions_for_beneficiary(
    db: Session,
    period_id: int,
    beneficiary_user_id: int,
    now: Optional[datetime] = None,
) -> int:
    """
    解锁某个用户在指定结算期的已资金化分成（locked -> available）。

    规则：
    - commission: funding_status=1 AND is_unlocked=0
    - beneficiary 在本期 payable.status=PAID(=2)
    - 同一批次必须走事务，保证 commission / ledger / wallet 三者一致

    返回：本次实际解锁的 coins（可能为 0）
    """
    if now is None:
        now = datetime.now()

    # 锁定符合条件的 commission 行，并计算本次可解锁总额
    row = (
        db.execute(
            text(
                """
                SELECT COALESCE(SUM(c.amount_coins), 0) AS sum_coins
                FROM settlement_commissions c
                JOIN settlement_user_payable p
                  ON p.period_id = c.period_id AND p.user_id = c.beneficiary_user_id
                WHERE c.period_id = :period_id
                  AND c.beneficiary_user_id = :beneficiary
                  AND c.funding_status = 1
                  AND c.is_unlocked = 0
                  AND p.status = 2
                FOR UPDATE
                """
            ),
            {"period_id": int(period_id), "beneficiary": int(beneficiary_user_id)},
        )
        .mappings()
        .first()
    )
    sum_coins = int((row or {}).get("sum_coins") or 0)
    if sum_coins <= 0:
        return 0

    # 锁定钱包行，确保 locked 足够（避免凭空造币）
    wallet = (
        db.execute(
            text(
                """
                SELECT available_coins, locked_coins
                FROM wallet_accounts
                WHERE user_id = :user_id
                FOR UPDATE
                """
            ),
            {"user_id": int(beneficiary_user_id)},
        )
        .mappings()
        .first()
    )
    if wallet is None:
        # 理论上：只有发生过 locked 入账才会有可解锁金额；若钱包不存在，属于数据不一致
        db.execute(
            text(
                """
                INSERT INTO wallet_accounts(user_id, available_coins, locked_coins)
                VALUES (:user_id, 0, 0)
                ON DUPLICATE KEY UPDATE user_id = user_id
                """
            ),
            {"user_id": int(beneficiary_user_id)},
        )
        wallet = (
            db.execute(
                text(
                    """
                    SELECT available_coins, locked_coins
                    FROM wallet_accounts
                    WHERE user_id = :user_id
                    FOR UPDATE
                    """
                ),
                {"user_id": int(beneficiary_user_id)},
            )
            .mappings()
            .first()
        )

    locked = int((wallet or {}).get("locked_coins") or 0)
    if locked < sum_coins:
        raise ValueError(
            f"解锁失败：钱包 locked 不足（user_id={beneficiary_user_id}, locked={locked}, need={sum_coins}）"
        )

    # 1) 标记 commission 已解锁
    db.execute(
        text(
            """
            UPDATE settlement_commissions
            SET is_unlocked = 1,
                unlocked_at = :now
            WHERE period_id = :period_id
              AND beneficiary_user_id = :beneficiary
              AND funding_status = 1
              AND is_unlocked = 0
            """
        ),
        {"now": now, "period_id": int(period_id), "beneficiary": int(beneficiary_user_id)},
    )

    # 2) 写入账本（locked -> available）
    db.execute(
        text(
            """
            INSERT INTO wallet_ledger(user_id, period_id, entry_type, delta_available_coins, delta_locked_coins, remark)
            VALUES (:user_id, :period_id, 'COMMISSION_UNLOCK', :sum_coins, :neg_sum, 'unlock after paid')
            """
        ),
        {
            "user_id": int(beneficiary_user_id),
            "period_id": int(period_id),
            "sum_coins": int(sum_coins),
            "neg_sum": -int(sum_coins),
        },
    )

    # 3) 更新账户余额
    db.execute(
        text(
            """
            UPDATE wallet_accounts
            SET available_coins = available_coins + :sum_coins,
                locked_coins = locked_coins - :sum_coins
            WHERE user_id = :user_id
            """
        ),
        {"user_id": int(beneficiary_user_id), "sum_coins": int(sum_coins)},
    )

    return sum_coins


def unlock_commissions_for_period(
    db: Session,
    period_id: int,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    批量解锁指定结算期内所有满足条件的分成。

    返回：
    - unlocked_users: 解锁到的受益人数量
    - unlocked_total_coins: 解锁总 coins
    """
    if now is None:
        now = datetime.now()

    rows = (
        db.execute(
            text(
                """
                SELECT c.beneficiary_user_id AS beneficiary_user_id
                FROM settlement_commissions c
                JOIN settlement_user_payable p
                  ON p.period_id = c.period_id AND p.user_id = c.beneficiary_user_id
                WHERE c.period_id = :period_id
                  AND c.funding_status = 1
                  AND c.is_unlocked = 0
                  AND p.status = 2
                GROUP BY c.beneficiary_user_id
                """
            ),
            {"period_id": int(period_id)},
        )
        .mappings()
        .all()
    )

    unlocked_users = 0
    unlocked_total_coins = 0

    for r in rows:
        beneficiary_user_id = int(r.get("beneficiary_user_id") or 0)
        if beneficiary_user_id <= 0:
            continue
        unlocked = unlock_commissions_for_beneficiary(
            db,
            int(period_id),
            beneficiary_user_id,
            now=now,
        )
        if unlocked > 0:
            unlocked_users += 1
            unlocked_total_coins += int(unlocked)

    return {
        "unlocked_users": unlocked_users,
        "unlocked_total_coins": unlocked_total_coins,
    }

