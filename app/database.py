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
    _add_column_if_not_exists(
        'settlement_periods',
        'is_active',
        'INT NOT NULL DEFAULT 0 COMMENT "是否为当前生效期：0=否 1=是（全局只能有一个为1）"',
    )
    _migrate_user_script_envs_user_id()
    _migrate_earning_records_user_id()


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


def _add_index_if_not_exists(table_name: str, index_name: str, columns_ddl: str) -> None:
    """如果索引不存在则添加索引（columns_ddl 例如: user_id 或 user_id,env_name）"""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = '{table_name}'
            AND INDEX_NAME = '{index_name}'
        """))
        exists = result.scalar() > 0
        if exists:
            return
        conn.execute(text(f"ALTER TABLE {table_name} ADD INDEX {index_name} ({columns_ddl})"))
        conn.commit()
        print(f"已添加索引: {table_name}.{index_name}")


def _add_foreign_key_if_not_exists(
    table_name: str,
    constraint_name: str,
    column_name: str,
    ref_table: str,
    ref_column: str,
) -> None:
    """如果外键不存在则添加外键"""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = '{table_name}'
            AND COLUMN_NAME = '{column_name}'
            AND REFERENCED_TABLE_NAME = '{ref_table}'
            AND REFERENCED_COLUMN_NAME = '{ref_column}'
        """))
        exists = (result.scalar() or 0) > 0
        if exists:
            return
        conn.execute(text(
            f"ALTER TABLE {table_name} "
            f"ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY ({column_name}) REFERENCES {ref_table}({ref_column})"
        ))
        conn.commit()
        print(f"已添加外键: {table_name}.{constraint_name}")


def _migrate_user_script_envs_user_id() -> None:
    """
    方案A：为 user_script_envs 增加 user_id 外键，并从 user_script_configs.user_id 回填。
    - 保留 config_id -> user_script_configs.id 不变
    - 新增 user_id -> users.id（便于直接按用户查询 env）
    """
    _add_column_if_not_exists('user_script_envs', 'user_id', 'BIGINT NULL COMMENT "归属用户（users.id）"')

    with engine.connect() as conn:
        # 回填（若已有值但与配置归属不一致，也进行修正）
        conn.execute(text("""
            UPDATE user_script_envs usev
            INNER JOIN user_script_configs usc ON usc.id = usev.config_id
            SET usev.user_id = usc.user_id
            WHERE usev.user_id IS NULL OR usev.user_id <> usc.user_id
        """))
        conn.commit()

        # 校验是否仍有空值（理论上不应存在）
        null_count = conn.execute(text("""
            SELECT COUNT(*) as count
            FROM user_script_envs
            WHERE user_id IS NULL
        """)).scalar() or 0
        if null_count:
            print(f"警告：user_script_envs.user_id 仍有 {null_count} 条记录为空，请检查数据完整性。")

    _add_index_if_not_exists('user_script_envs', 'idx_user_script_envs_user_id', 'user_id')
    with engine.connect() as conn:
        orphan_count = conn.execute(text("""
            SELECT COUNT(*) as count
            FROM user_script_envs usev
            LEFT JOIN users u ON u.id = usev.user_id
            WHERE usev.user_id IS NOT NULL AND u.id IS NULL
        """)).scalar() or 0
        if orphan_count:
            print(
                f"警告：user_script_envs.user_id 存在 {orphan_count} 条孤儿记录（users 中不存在对应 id），"
                f"已跳过添加外键 fk_user_script_envs_user_id，请先修复数据后重启再试。"
            )
            return

    try:
        _add_foreign_key_if_not_exists('user_script_envs', 'fk_user_script_envs_user_id', 'user_id', 'users', 'id')
    except Exception as exc:
        print(f"警告：添加外键 fk_user_script_envs_user_id 失败，已跳过。原因: {exc}")


def _migrate_earning_records_user_id() -> None:
    """
    为 earning_records 增加 user_id 外键，并按 earning_records.env_id -> user_script_envs.user_id 回填。
    """
    _add_column_if_not_exists('earning_records', 'user_id', 'BIGINT NULL COMMENT "归属用户（users.id）"')

    _add_index_if_not_exists('earning_records', 'idx_earning_records_user_id', 'user_id')

    # 回填只在确实存在空值时执行，避免每次启动全表 UPDATE
    with engine.connect() as conn:
        needs_backfill = conn.execute(text("""
            SELECT EXISTS(
                SELECT 1 FROM earning_records WHERE user_id IS NULL
            ) as needs_backfill
        """)).scalar() or 0
        if not needs_backfill:
            # 已回填完成/后续写入已自动带入 user_id，无需重复回填
            pass
        else:
            conn.execute(text("""
                UPDATE earning_records er
                INNER JOIN user_script_envs usev ON usev.id = er.env_id
                SET er.user_id = usev.user_id
                WHERE er.user_id IS NULL
            """))
            conn.commit()

            null_count = conn.execute(text("""
                SELECT COUNT(*) as count
                FROM earning_records
                WHERE user_id IS NULL
            """)).scalar() or 0
            if null_count:
                print(f"警告：earning_records.user_id 仍有 {null_count} 条记录为空，请检查 user_script_envs.user_id 是否完整。")

    with engine.connect() as conn:
        orphan_count = conn.execute(text("""
            SELECT COUNT(*) as count
            FROM earning_records er
            LEFT JOIN users u ON u.id = er.user_id
            WHERE er.user_id IS NOT NULL AND u.id IS NULL
        """)).scalar() or 0
        if orphan_count:
            print(
                f"警告：earning_records.user_id 存在 {orphan_count} 条孤儿记录（users 中不存在对应 id），"
                f"已跳过添加外键 fk_earning_records_user_id，请先修复数据后重启再试。"
            )
            return

    try:
        _add_foreign_key_if_not_exists('earning_records', 'fk_earning_records_user_id', 'user_id', 'users', 'id')
    except Exception as exc:
        print(f"警告：添加外键 fk_earning_records_user_id 失败，已跳过。原因: {exc}")

