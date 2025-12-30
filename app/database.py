import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# MySQL数据库连接配置（从环境变量读取）
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://root:My20010228%40@192.168.5.204:3307/get_petty_advantages?charset=utf8mb4"
)

# 创建数据库引擎
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    echo=False
)

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建基础模型类
Base = declarative_base()


def get_db():
    """获取数据库会话"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """初始化数据库，创建所有表"""
    from app.models import UserScriptEnv

    Base.metadata.create_all(bind=engine)

    # 添加禁用恢复相关的新字段（如果不存在）
    _add_column_if_not_exists('user_script_envs', 'disabled_until', 'DATETIME NULL COMMENT "禁用至何时，到期自动恢复"')
    _add_column_if_not_exists('user_script_envs', 'disable_days', 'INT NULL COMMENT "禁用天数（3/5/7）"')
    _add_column_if_not_exists('user_script_envs', 'disabled_at', 'DATETIME NULL COMMENT "禁用开始时间"')


def _add_column_if_not_exists(table_name: str, column_name: str, column_definition: str):
    """如果列不存在则添加列"""
    with engine.connect() as conn:
        # 检查列是否存在
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = '{table_name}'
            AND COLUMN_NAME = '{column_name}'
        """))
        exists = result.scalar() > 0

        if not exists:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"))
            conn.commit()
            print(f"已添加列: {table_name}.{column_name}")

