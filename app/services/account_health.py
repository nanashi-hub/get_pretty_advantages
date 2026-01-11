from datetime import date, timedelta

from sqlalchemy.orm import Session

from app.models import EarningRecord


def pick_account_health_basis(db: Session) -> tuple[date, str, str]:
    """选择账号状态统计日：今日有数据用今日，否则用昨日"""
    today = date.today()
    has_today = (
        db.query(EarningRecord.stat_date)
        .filter(EarningRecord.stat_date == today)
        .limit(1)
        .first()
        is not None
    )
    stat_date = today if has_today else (today - timedelta(days=1))
    basis = "today" if has_today else "yesterday"
    basis_label = "今日" if has_today else "昨日"
    return stat_date, basis, basis_label


def classify_account_health(has_data: bool, coins: int) -> tuple[str, str]:
    """按统计日金币分类账号状态"""
    if not has_data:
        return "no_data", "未统计"
    if coins <= 0:
        return "need_config", "需更换配置"
    if coins < 500:
        return "black", "黑号"
    if coins < 10000:
        return "edge", "边缘"
    return "normal", "正常"

