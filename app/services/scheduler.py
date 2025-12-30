"""
定时任务调度器
使用 APScheduler 实现定时检查支付状态
"""
import logging
from contextlib import contextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.database import SessionLocal
from app.services.alipay_service import check_pending_payments

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
