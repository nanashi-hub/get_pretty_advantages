"""
定时任务调度器
使用 APScheduler 实现定时检查支付状态
"""
import logging
import os
from contextlib import contextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from app.database import SessionLocal
from app.services.alipay_service import check_pending_payments
from app.services.ksck_cleanup import archive_need_config_streak_envs

logger = logging.getLogger(__name__)
scheduler = BackgroundScheduler()


@contextmanager
def get_db_session():
    """获取数据库会话的上下文管理器"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def payment_check_job():
    """
    定时检查支付状态的任务

    每30秒执行一次，检查待支付订单
    """
    try:
        with get_db_session() as db:
            result = check_pending_payments(db)

            if result.get("confirmed_orders", 0) > 0:
                logger.info(
                    f"支付检查完成: 检查 {result.get('checked_orders', 0)} 个订单, "
                    f"确认 {result.get('confirmed_orders', 0)} 个"
                )
    except Exception as e:
        logger.error(f"支付检查任务执行失败: {e}")


def ksck_need_config_cleanup_job(days: int):
    """归档连续 N 天需更换配置的 ksck 账号（可选启用）"""
    try:
        with get_db_session() as db:
            result = archive_need_config_streak_envs(db, days=days, dry_run=False, delete_in_qinglong=True)
            if result.archived > 0 or result.ql_delete_failed > 0:
                logger.info(
                    "ksck 自动归档完成: days=%s, window=%s..%s, candidates=%s, archived=%s, ql_deleted=%s, ql_failed=%s",
                    result.days,
                    result.stat_start_date,
                    result.stat_end_date,
                    result.candidates,
                    result.archived,
                    result.ql_deleted,
                    result.ql_delete_failed,
                )
    except Exception as e:
        logger.error(f"ksck 自动归档任务执行失败: {e}")


def start_scheduler():
    """启动定时调度器"""
    if not scheduler.running:
        # 添加支付检查任务：每30秒执行一次
        scheduler.add_job(
            payment_check_job,
            IntervalTrigger(seconds=30),
            id="payment_check",
            name="检查支付宝支付状态",
            replace_existing=True
        )

        # ksck 自动归档（默认关闭，设置 KSCK_AUTO_CLEANUP_DAYS=15 开启）
        try:
            days = int(os.getenv("KSCK_AUTO_CLEANUP_DAYS", "0") or "0")
        except Exception:
            days = 0
        if days > 0:
            scheduler.add_job(
                ksck_need_config_cleanup_job,
                CronTrigger(hour=4, minute=10),
                id="ksck_need_config_cleanup",
                name=f"归档连续{days}天需更换配置账号",
                replace_existing=True,
                kwargs={"days": days},
            )

        scheduler.start()
        logger.info("定时调度器已启动")


def stop_scheduler():
    """停止定时调度器"""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("定时调度器已停止")


def get_scheduler_status():
    """获取调度器状态"""
    return {
        "running": scheduler.running,
        "jobs": [
            {
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None
            }
            for job in scheduler.get_jobs()
        ]
    }
