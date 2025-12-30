"""
日志配置模块
配置日志输出到文件和控制台
"""
import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path


# 日志目录
LOG_DIR = Path("/app/logs")
# 如果是本地开发环境，日志目录可能不同
if not LOG_DIR.exists() and Path("./logs").exists():
    LOG_DIR = Path("./logs")


def setup_logging(
    level: str = "INFO",
    log_dir: Path = LOG_DIR,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
) -> None:
    """
    配置应用日志

    Args:
        level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: 日志存储目录
        max_bytes: 单个日志文件最大字节数
        backup_count: 保留的日志文件数量
    """
    # 确保日志目录存在
    log_dir.mkdir(parents=True, exist_ok=True)

    # 获取 root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # 清除已有的 handlers
    root_logger.handlers.clear()

    # 日志格式
    log_format = logging.Formatter(
        fmt="%(asctime)s [%(levelname)8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 详细格式（用于文件日志，包含异常堆栈）
    detailed_format = logging.Formatter(
        fmt="%(asctime)s [%(levelname)8s] [%(name)s:%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # ==================== 控制台 Handler ====================
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)

    # ==================== 文件 Handlers ====================

    # 1. 所有日志文件（包含 DEBUG 级别）
    all_log_file = log_dir / "app_all.log"
    all_handler = RotatingFileHandler(
        all_log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    all_handler.setLevel(logging.DEBUG)
    all_handler.setFormatter(detailed_format)
    root_logger.addHandler(all_handler)

    # 2. 错误日志文件（只记录 ERROR 及以上）
    error_log_file = log_dir / "app_error.log"
    error_handler = RotatingFileHandler(
        error_log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_format)
    root_logger.addHandler(error_handler)

    # 3. 按时间轮转的日志文件（每天一个文件）
    daily_log_file = log_dir / "app_daily.log"
    daily_handler = TimedRotatingFileHandler(
        daily_log_file,
        when="midnight",
        interval=1,
        backupCount=30,  # 保留30天
        encoding="utf-8"
    )
    daily_handler.suffix = "%Y-%m-%d"
    daily_handler.setLevel(logging.INFO)
    daily_handler.setFormatter(detailed_format)
    root_logger.addHandler(daily_handler)

    # 第三方库日志级别控制
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的 logger

    Args:
        name: logger 名称，通常使用 __name__

    Returns:
        Logger 实例
    """
    return logging.getLogger(name)


# 环境变量控制日志级别
def setup_logging_from_env() -> None:
    """从环境变量读取配置并设置日志"""
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_dir = Path(os.getenv("LOG_DIR", str(LOG_DIR)))
    max_bytes = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 默认10MB
    backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))

    setup_logging(
        level=log_level,
        log_dir=log_dir,
        max_bytes=max_bytes,
        backup_count=backup_count
    )
