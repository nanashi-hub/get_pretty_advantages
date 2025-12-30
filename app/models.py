from sqlalchemy import Column, Integer, String, Boolean, DateTime, Enum, BigInteger, Text, Date, DECIMAL, ForeignKey, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum
from app.database import Base


class UserRole(str, enum.Enum):
    """用户角色枚举"""
    ADMIN = "admin"      # 平台管理员
    AGENT = "agent"      # 代理(+1/+2)
    NORMAL = "normal"    # 普通用户/号主


class ConfigStatus(str, enum.Enum):
    """配置状态枚举"""
    ENABLED = "enabled"
    DISABLED = "disabled"


class EnvStatus(str, enum.Enum):
    """环境变量状态枚举"""
    VALID = "valid"
    INVALID = "invalid"


class KSAccountStatus(str, enum.Enum):
    """快手账号状态枚举"""
    NORMAL = "normal"    # 正常
    BLACK = "black"      # 黑号
    BANNED = "banned"    # 封禁
    EXPIRED = "expired"  # CK失效


class RunLogStatus(str, enum.Enum):
    """运行日志状态枚举"""
    SUCCESS = "success"
    FAIL = "fail"
    PARTIAL = "partial"


class SettlementPeriodStatus(str, enum.Enum):
    """结算周期状态枚举"""
    OPEN = "open"
    CLOSED = "closed"


class SettlementStatus(str, enum.Enum):
    """结算状态枚举"""
    PENDING = "pending"
    PAID = "paid"
    CANCELLED = "cancelled"


class TransactionType(str, enum.Enum):
    """钱包交易类型枚举"""
    SETTLEMENT_INCOME = "settlement_income"  # 结算收入
    INVITE_REWARD = "invite_reward"          # 邀请奖励
    WITHDRAW = "withdraw"                    # 提现
    ADJUST = "adjust"                        # 调整
    RECHARGE = "recharge"                    # 充值
    SETTLEMENT_DISTRIBUTE = "settlement_distribute"  # 结算分发（分账）


class RechargeOrderStatus(str, enum.Enum):
    """充值订单状态枚举"""
    PENDING = "pending"      # 待支付
    PAID = "paid"           # 已支付
    CONFIRMED = "confirmed"  # 已确认（已分账）
    CANCELLED = "cancelled"  # 已取消
    EXPIRED = "expired"      # 已过期


class TransferStatus(str, enum.Enum):
    """转账状态枚举"""
    PENDING = "pending"      # 待转账
    PROCESSING = "processing"  # 转账中
    SUCCESS = "success"      # 成功
    FAILED = "failed"        # 失败
    REFUNDED = "refunded"    # 已退回


# ==================== 用户与登录模块 ====================

class User(Base):
    """用户模型 - 对应数据库users表"""
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    username = Column(String(50), unique=True, index=True, nullable=False, comment="登录名")
    password_hash = Column(String(255), nullable=False, comment="密码哈希（如bcrypt）")
    nickname = Column(String(50), nullable=True, comment="昵称/备注名")
    phone = Column(String(20), nullable=True, comment="手机号")
    wechat_id = Column(String(50), nullable=True, comment="微信ID（用于联系/结算）")
    alipay_account = Column(String(100), nullable=True, comment="支付宝账号（用于分账收款）")
    role = Column(
        Enum(UserRole, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=UserRole.NORMAL,
        comment="admin=平台管理员; agent=代理(+1/+2); normal=普通用户/号主"
    )
    # 推广相关字段
    referral_code = Column(String(100), unique=True, nullable=True, comment="我的推广码")
    inviter_id = Column(BigInteger, ForeignKey("users.id"), nullable=True, comment="直接邀请人ID（+1）")
    status = Column(Integer, nullable=False, default=1, comment="1=正常,0=禁用")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    referral = relationship("UserReferral", back_populates="user", uselist=False, foreign_keys="UserReferral.user_id")
    script_configs = relationship("UserScriptConfig", back_populates="user")
    ks_accounts = relationship("KSAccount", back_populates="user")
    wallet = relationship("WalletAccount", back_populates="user", uselist=False)

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', role='{self.role.value}')>"


# ==================== 推广关系模块 ====================

class UserReferral(Base):
    """推广关系表 - 对应数据库user_referrals表"""
    __tablename__ = "user_referrals"

    user_id = Column(BigInteger, ForeignKey("users.id"), primary_key=True, comment="被邀请人（号主）")
    inviter_level1 = Column(BigInteger, ForeignKey("users.id"), nullable=True, comment="+1，直接邀请人")
    inviter_level2 = Column(BigInteger, ForeignKey("users.id"), nullable=True, comment="+2，邀请+1的人")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # 关系
    user = relationship("User", back_populates="referral", foreign_keys=[user_id])
    inviter1 = relationship("User", foreign_keys=[inviter_level1])
    inviter2 = relationship("User", foreign_keys=[inviter_level2])

    def __repr__(self):
        return f"<UserReferral(user_id={self.user_id}, inviter_level1={self.inviter_level1}, inviter_level2={self.inviter_level2})>"


# ==================== 青龙实例 & 脚本配置模块 ====================

class QLInstance(Base):
    """青龙实例表"""
    __tablename__ = "ql_instances"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    name = Column(String(50), nullable=False, comment="青龙实例名称")
    base_url = Column(String(255), nullable=False, comment="http://ip:5700")
    client_id = Column(String(100), nullable=False, comment="青龙应用 client_id")
    client_secret = Column(String(100), nullable=False, comment="青龙应用 client_secret")
    remark = Column(String(255), nullable=True, comment="备注")
    status = Column(Integer, nullable=False, default=1, comment="1=可用,0=停用")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    script_configs = relationship("UserScriptConfig", back_populates="ql_instance")

    def __repr__(self):
        return f"<QLInstance(id={self.id}, name='{self.name}')>"


class UserScriptConfig(Base):
    """用户脚本配置主表"""
    __tablename__ = "user_script_configs"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False, comment="归属用户（号主）")
    ql_instance_id = Column(BigInteger, ForeignKey("ql_instances.id"), nullable=False, comment="对应青龙实例")
    script_name = Column(String(100), nullable=False, comment="脚本名称/标识，如 ks_gold.js")
    group_key = Column(String(100), nullable=False, comment="一组配置的key，用于关联多个变量")
    status = Column(
        Enum(ConfigStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=ConfigStatus.ENABLED,
        comment="这组配置整体是否允许跑脚本"
    )
    last_sync_at = Column(DateTime(timezone=True), nullable=True, comment="最近一次同步到青龙的时间")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    user = relationship("User", back_populates="script_configs")
    ql_instance = relationship("QLInstance", back_populates="script_configs")
    envs = relationship("UserScriptEnv", back_populates="config", cascade="all, delete-orphan")
    switch = relationship("ScriptRunSwitch", back_populates="config", uselist=False, cascade="all, delete-orphan")
    run_logs = relationship("ScriptRunLog", back_populates="config")

    def __repr__(self):
        return f"<UserScriptConfig(id={self.id}, user_id={self.user_id}, script_name='{self.script_name}')>"


class UserScriptEnv(Base):
    """用户环境变量表"""
    __tablename__ = "user_script_envs"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    config_id = Column(BigInteger, ForeignKey("user_script_configs.id"), nullable=False, comment="user_script_configs.id")
    env_name = Column(String(100), nullable=False, index=True, comment="环境变量名，例如 KS_COOKIE")
    env_value = Column(Text, nullable=False, comment="变量值，例如 CK")
    ql_env_id = Column(String(100), nullable=True, comment="在青龙中的 env id")
    ip_id = Column(BigInteger, ForeignKey("ip_pool.id"), nullable=True, comment="引用的代理IP")
    status = Column(
        Enum(EnvStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=EnvStatus.VALID,
        comment="此变量有效性"
    )
    remark = Column(String(255), nullable=True, comment="备注")
    # 禁用恢复相关字段
    disabled_until = Column(DateTime(timezone=True), nullable=True, comment="禁用至何时，到期自动恢复")
    disable_days = Column(Integer, nullable=True, comment="禁用天数（3/5/7）")
    disabled_at = Column(DateTime(timezone=True), nullable=True, comment="禁用开始时间")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    config = relationship("UserScriptConfig", back_populates="envs")
    ip = relationship("IPPool")

    def __repr__(self):
        return f"<UserScriptEnv(id={self.id}, env_name='{self.env_name}')>"


# ==================== 快手账号 & CK 管理模块 ====================

class KSAccount(Base):
    """快手账号表"""
    __tablename__ = "ks_accounts"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False, comment="号主（users.id）")
    config_id = Column(BigInteger, ForeignKey("user_script_configs.id"), nullable=True, comment="关联配置")
    mobile = Column(String(20), nullable=True, comment="快手绑定手机号")
    ks_uid = Column(String(50), nullable=True, comment="快手用户ID")
    current_ck = Column(Text, nullable=True, comment="当前生效的CK")
    status = Column(
        Enum(KSAccountStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=KSAccountStatus.NORMAL,
        comment="normal=正常; black=黑号; banned=封禁; expired=CK失效"
    )
    last_ck_refresh_at = Column(DateTime(timezone=True), nullable=True, comment="最近刷新CK时间")
    last_run_at = Column(DateTime(timezone=True), nullable=True, comment="最近跑脚本时间")
    device_info = Column(String(255), nullable=True, comment="设备标识/备注")
    ip_group = Column(String(50), nullable=True, comment="使用的IP分组/线路")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    user = relationship("User", back_populates="ks_accounts")
    config = relationship("UserScriptConfig")
    earning_records = relationship("EarningRecord", back_populates="ks_account")
    run_logs = relationship("ScriptRunLog", back_populates="ks_account")

    def __repr__(self):
        return f"<KSAccount(id={self.id}, user_id={self.user_id}, mobile='{self.mobile}')>"


# ==================== 脚本开关与运行日志模块 ====================

class ScriptRunSwitch(Base):
    """开关/计划表"""
    __tablename__ = "script_run_switches"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    config_id = Column(BigInteger, ForeignKey("user_script_configs.id"), nullable=False, comment="user_script_configs.id")
    is_enabled = Column(Integer, nullable=False, default=1, comment="1=开，0=关")
    cron_expr = Column(String(100), nullable=True, comment="cron 表达式")
    max_daily_runs = Column(Integer, nullable=True, comment="每日最大运行次数限制")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    config = relationship("UserScriptConfig", back_populates="switch")

    def __repr__(self):
        return f"<ScriptRunSwitch(id={self.id}, config_id={self.config_id}, is_enabled={self.is_enabled})>"


class ScriptRunLog(Base):
    """运行日志表"""
    __tablename__ = "script_run_logs"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    config_id = Column(BigInteger, ForeignKey("user_script_configs.id"), nullable=False)
    ks_account_id = Column(BigInteger, ForeignKey("ks_accounts.id"), nullable=True, comment="对应具体快手号")
    task_name = Column(String(100), nullable=False, comment="脚本内任务名称")
    run_at = Column(DateTime(timezone=True), nullable=False, index=True, comment="运行时间")
    status = Column(
        Enum(RunLogStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=RunLogStatus.SUCCESS
    )
    coins_earned = Column(Integer, nullable=False, default=0, comment="本次获得金币数")
    raw_log_snippet = Column(Text, nullable=True, comment="原始日志摘录")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # 关系
    config = relationship("UserScriptConfig", back_populates="run_logs")
    ks_account = relationship("KSAccount", back_populates="run_logs")

    def __repr__(self):
        return f"<ScriptRunLog(id={self.id}, task_name='{self.task_name}', status='{self.status}')>"


# ==================== 金币收益 & 结算体系模块 ====================

class EarningRecord(Base):
    """日维度收益表"""
    __tablename__ = "earning_records"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    ks_account_id = Column(BigInteger, ForeignKey("ks_accounts.id"), nullable=False, comment="对应快手账号")
    stat_date = Column(Date, nullable=False, comment="统计日期")
    coins_total = Column(BigInteger, nullable=False, default=0, comment="当天总金币")
    coins_from_food = Column(BigInteger, nullable=False, default=0)
    coins_from_look = Column(BigInteger, nullable=False, default=0)
    coins_from_box = Column(BigInteger, nullable=False, default=0)
    coins_from_search = Column(BigInteger, nullable=False, default=0)
    remark = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    ks_account = relationship("KSAccount", back_populates="earning_records")

    def __repr__(self):
        return f"<EarningRecord(id={self.id}, ks_account_id={self.ks_account_id}, stat_date={self.stat_date})>"


class SettlementPeriod(Base):
    """结算周期表"""
    __tablename__ = "settlement_periods"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    period_label = Column(String(50), nullable=False, unique=True, comment="例如 2025W01")
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    status = Column(
        Enum(SettlementPeriodStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=SettlementPeriodStatus.OPEN
    )
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # 关系
    details = relationship("SettlementDetail", back_populates="period")

    def __repr__(self):
        return f"<SettlementPeriod(id={self.id}, period_label='{self.period_label}')>"


class SettlementDetail(Base):
    """结算明细表"""
    __tablename__ = "settlement_details"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    period_id = Column(BigInteger, ForeignKey("settlement_periods.id"), nullable=False, comment="settlement_periods.id")
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False, comment="号主用户ID")
    coins_total = Column(BigInteger, nullable=False, default=0, comment="本周期总金币")
    rate_per_10k = Column(DECIMAL(10, 2), nullable=False, default=1.00, comment="每1万金币兑换金额")
    amount_total = Column(DECIMAL(10, 2), nullable=False, default=0, comment="应支付总金额")
    amount_to_user = Column(DECIMAL(10, 2), nullable=False, default=0, comment="支付给号主")
    amount_to_level1 = Column(DECIMAL(10, 2), nullable=False, default=0, comment="+1 分成金额")
    amount_to_level2 = Column(DECIMAL(10, 2), nullable=False, default=0, comment="+2 分成金额")
    status = Column(
        Enum(SettlementStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=SettlementStatus.PENDING
    )
    settled_at = Column(DateTime(timezone=True), nullable=True, comment="实际结算时间")
    remark = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    period = relationship("SettlementPeriod", back_populates="details")
    user = relationship("User")

    def __repr__(self):
        return f"<SettlementDetail(id={self.id}, user_id={self.user_id}, coins_total={self.coins_total})>"


class WalletAccount(Base):
    """钱包账户表"""
    __tablename__ = "wallet_accounts"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False, unique=True)
    balance = Column(DECIMAL(12, 2), nullable=False, default=0, comment="当前余额")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # 关系
    user = relationship("User", back_populates="wallet")
    transactions = relationship("WalletTransaction", back_populates="wallet")

    def __repr__(self):
        return f"<WalletAccount(id={self.id}, user_id={self.user_id}, balance={self.balance})>"


class WalletTransaction(Base):
    """钱包流水表"""
    __tablename__ = "wallet_transactions"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    wallet_id = Column(BigInteger, ForeignKey("wallet_accounts.id"), nullable=True)
    amount = Column(DECIMAL(12, 2), nullable=False, comment="正数=入账, 负数=扣款")
    type = Column(
        Enum(TransactionType, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False
    )
    ref_id = Column(BigInteger, nullable=True, comment="关联ID")
    description = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # 关系
    wallet = relationship("WalletAccount", back_populates="transactions")
    user = relationship("User")

    def __repr__(self):
        return f"<WalletTransaction(id={self.id}, user_id={self.user_id}, amount={self.amount})>"


# ==================== IP 池 ====================

class IPPool(Base):
    """代理 IP 池"""
    __tablename__ = "ip_pool"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    ip = Column(String(45), nullable=False, comment="IP地址")
    port = Column(Integer, nullable=False, comment="端口")
    username = Column(String(100), nullable=True, comment="代理账号")
    password = Column(String(100), nullable=True, comment="代理密码")
    proxy_url = Column(String(255), nullable=True, comment="调用格式/代理URL")
    region = Column(String(50), nullable=True, comment="地区/城市")
    expire_date = Column(Date, nullable=True, comment="到期时间")
    vendor = Column(String(100), nullable=True, comment="IP厂商/供应商")
    max_users = Column(Integer, nullable=False, default=2, comment="最多同时使用人数")
    status = Column(String(20), nullable=False, default="active", comment="active/disabled")
    usage_count = Column(Integer, nullable=False, default=0, comment="当前使用次数（代码维护）")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<IPPool(id={self.id}, ip={self.ip}, port={self.port})>"


# ==================== API 管理与操作审计模块（可选） ====================

class APIKey(Base):
    """API 密钥表"""
    __tablename__ = "api_keys"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    api_key = Column(String(100), nullable=False, unique=True)
    daily_limit = Column(Integer, nullable=True, comment="每日调用上限")
    total_limit = Column(Integer, nullable=True, comment="生命周期总调用上限")
    calls_today = Column(Integer, nullable=False, default=0)
    calls_total = Column(Integer, nullable=False, default=0)
    last_call_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(Integer, nullable=False, default=1, comment="1=启用,0=禁用")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    user = relationship("User")

    def __repr__(self):
        return f"<APIKey(id={self.id}, user_id={self.user_id})>"


class OperationLog(Base):
    """操作日志表"""
    __tablename__ = "operation_logs"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    action = Column(String(50), nullable=False, index=True, comment="动作名称")
    target_type = Column(String(50), nullable=True, comment="操作对象类型")
    target_id = Column(BigInteger, nullable=True, comment="操作对象ID")
    ip_address = Column(String(45), nullable=True)
    detail = Column(JSON, nullable=True, comment="更多参数")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # 关系
    user = relationship("User")

    def __repr__(self):
        return f"<OperationLog(id={self.id}, user_id={self.user_id}, action='{self.action}')>"


# ==================== 充值与分账模块 ====================

class RechargeOrder(Base):
    """充值订单表"""
    __tablename__ = "recharge_orders"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    order_no = Column(String(50), unique=True, nullable=False, index=True, comment="平台订单号")
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False, comment="充值用户ID")
    amount = Column(DECIMAL(10, 2), nullable=False, comment="充值金额")
    remark_in = Column(String(200), nullable=True, comment="付款备注（用户填写订单号）")

    # 支付宝相关
    alipay_trade_no = Column(String(100), unique=True, nullable=True, comment="支付宝交易号")
    alipay_log_id = Column(String(100), nullable=True, comment="支付宝日志ID")

    status = Column(
        Enum(RechargeOrderStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=RechargeOrderStatus.PENDING
    )

    # 时间字段
    paid_at = Column(DateTime(timezone=True), nullable=True, comment="支付时间")
    confirmed_at = Column(DateTime(timezone=True), nullable=True, comment="确认时间（分账完成）")
    expired_at = Column(DateTime(timezone=True), nullable=True, comment="过期时间")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    user = relationship("User")
    transfers = relationship("TransferRecord", back_populates="recharge_order")

    def __repr__(self):
        return f"<RechargeOrder(id={self.id}, order_no='{self.order_no}', status='{self.status}')>"


class TransferRecord(Base):
    """转账记录表（分账明细）"""
    __tablename__ = "transfer_records"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    recharge_order_id = Column(BigInteger, ForeignKey("recharge_orders.id"), nullable=False, comment="关联充值订单")
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False, comment="收款用户ID")

    amount = Column(DECIMAL(10, 2), nullable=False, comment="转账金额")
    role = Column(String(20), nullable=False, comment="角色: user/agent_l1/agent_l2/platform")
    alipay_account = Column(String(100), nullable=False, comment="收款支付宝账号")

    # 支付宝转账相关
    alipay_order_id = Column(String(100), unique=True, nullable=True, comment="支付宝转账单号")
    alipay_status = Column(String(50), nullable=True, comment="支付宝返回的状态")

    status = Column(
        Enum(TransferStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
        default=TransferStatus.PENDING
    )

    fail_reason = Column(String(255), nullable=True, comment="失败原因")
    transferred_at = Column(DateTime(timezone=True), nullable=True, comment="转账完成时间")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    recharge_order = relationship("RechargeOrder", back_populates="transfers")
    user = relationship("User")

    def __repr__(self):
        return f"<TransferRecord(id={self.id}, user_id={self.user_id}, amount={self.amount})>"


class AlipayConfig(Base):
    """支付宝配置表"""
    __tablename__ = "alipay_config"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    name = Column(String(50), nullable=False, comment="配置名称")
    app_id = Column(String(50), nullable=False, unique=True, comment="支付宝应用ID")
    private_key = Column(Text, nullable=False, comment="应用私钥")
    alipay_public_key = Column(Text, nullable=False, comment="支付宝公钥")
    sign_type = Column(String(10), nullable=False, default="RSA2", comment="签名方式 RSA/RSA2")
    gateway = Column(String(100), nullable=False, default="https://openapi.alipay.com/gateway.do", comment="网关地址")
    qrcode_url = Column(String(255), nullable=True, comment="收款码图片URL")
    alipay_account = Column(String(100), nullable=True, comment="平台支付宝账号")

    # 分账配置
    platform_fee_rate = Column(DECIMAL(5, 4), nullable=False, default=0.1000, comment="平台抽成比例（0.1=10%）")
    agent_l1_rate = Column(DECIMAL(5, 4), nullable=False, default=0.5400, comment="一级代理分成比例")
    agent_l2_rate = Column(DECIMAL(5, 4), nullable=False, default=0.2700, comment="二级代理分成比例")
    user_rate = Column(DECIMAL(5, 4), nullable=False, default=0.0900, comment="号主分成比例")

    status = Column(Integer, nullable=False, default=1, comment="1=启用, 0=禁用")
    remark = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<AlipayConfig(id={self.id}, name='{self.name}', app_id='{self.app_id}')>"
