from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    DECIMAL,
    Enum,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
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
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=True, index=True, comment="归属用户（users.id）")
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
    user = relationship("User")
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

    env_id = Column(
        BigInteger,
        ForeignKey("user_script_envs.id"),
        nullable=False,
        index=True,
        comment="FK -> user_script_envs.id",
    )
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=True, index=True, comment="归属用户（users.id）")
    stat_date = Column(Date, primary_key=True, nullable=False, comment="统计日期（按天）")
    account_remark = Column(String(255), primary_key=True, nullable=False, comment="当日统计口径账号标识（快照）")

    coins_total = Column(BigInteger, nullable=False, default=0, comment="当日总金币")
    coins_from_look = Column(BigInteger, nullable=False, default=0, comment="look：看广告得金币")
    coins_from_lookk = Column(BigInteger, nullable=False, default=0, comment="lookk：看广告得奖励")
    coins_from_dj = Column(BigInteger, nullable=False, default=0, comment="dj：看短剧广告")
    coins_from_food = Column(BigInteger, nullable=False, default=0, comment="food：饭补广告")
    coins_from_box = Column(BigInteger, nullable=False, default=0, comment="box：宝箱广告")
    coins_from_search = Column(BigInteger, nullable=False, default=0, comment="search：搜索广告")

    record_note = Column(String(255), nullable=True, comment="记录备注（调试/回填说明/来源等）")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<EarningRecord(env_id={self.env_id}, stat_date={self.stat_date}, account_remark='{self.account_remark}')>"


class SettlementPeriod(Base):
    """结算期主表：定义统计区间、缴费窗口与本期规则参数（历史可复算）"""
    __tablename__ = "settlement_periods"

    period_id = Column(BigInteger, primary_key=True, index=True, autoincrement=True, comment="结算期ID（主键，自增）")
    period_start = Column(Date, nullable=False, comment="收益统计开始日（例如上月1号）")
    period_end = Column(Date, nullable=False, comment="收益统计结束日（例如上月月末）")
    pay_start = Column(Date, nullable=False, comment="缴费窗口开始日（例如本月1号）")
    pay_end = Column(Date, nullable=False, comment="缴费窗口截止日（例如本月10号，含当日）")

    coin_rate = Column(Integer, nullable=False, default=10000, comment="金币兑人民币比例：coin_rate coins = 1 元")
    host_bps = Column(Integer, nullable=False, default=6000, comment="号主自留比例（万分比bps），6000=60%")
    l1_bps = Column(Integer, nullable=False, default=2000, comment="+1 分成比例（bps），2000=20%")
    l2_bps = Column(Integer, nullable=False, default=400, comment="+2 分成比例（bps），400=4%")
    collect_bps = Column(Integer, nullable=False, default=4000, comment="号主应缴平台比例（bps），4000=40%")

    status = Column(Integer, nullable=False, default=1, comment="结算期状态：0=OPEN 1=PAYING 2=CLOSED")
    is_active = Column(Integer, nullable=False, default=0, comment="是否为当前生效期：0=否 1=是（全局只能有一个为1）")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("period_start", "period_end", name="uk_period_range"),
    )

    @hybrid_property
    def period_label(self) -> str:
        """生成周期标识，如 2025W01 或 2025-01"""
        if self.period_start and self.period_end:
            # 尝试判断是否为周周期（日期间隔 <= 7天）
            days_diff = (self.period_end - self.period_start).days
            if days_diff <= 7:
                # 周周期格式：2025W01
                start = self.period_start
                week_num = start.isocalendar()[1]
                return f"{start.year}W{week_num:02d}"
            else:
                # 月周期格式：2025-01
                return f"{self.period_start.year}-{self.period_start.month:02d}"
        return str(self.period_id)

    def __repr__(self):
        return f"<SettlementPeriod(period_id={self.period_id}, period_start={self.period_start}, period_end={self.period_end})>"


class SettlementReferralSnapshot(Base):
    """结算关系快照表：冻结每期用户的+1/+2关系，保证历史结算可复算"""
    __tablename__ = "settlement_referral_snapshot"

    period_id = Column(BigInteger, primary_key=True, comment="结算期ID（settlement_periods.period_id）")
    user_id = Column(BigInteger, primary_key=True, comment="用户ID（本期被结算的用户）")
    inviter_level1 = Column(BigInteger, nullable=True, comment="本期快照下的一级上级（+1）user_id")
    inviter_level2 = Column(BigInteger, nullable=True, comment="本期快照下的二级上级（+2）user_id")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="快照生成时间")

    __table_args__ = (
        Index("idx_snap_l1", "period_id", "inviter_level1"),
        Index("idx_snap_l2", "period_id", "inviter_level2"),
    )

    def __repr__(self):
        return f"<SettlementReferralSnapshot(period_id={self.period_id}, user_id={self.user_id})>"


class SettlementUserIncome(Base):
    """结算期用户收益汇总表：按期汇总用户总金币及拆分，作为后续应缴/分成/对账事实来源"""
    __tablename__ = "settlement_user_income"

    period_id = Column(BigInteger, primary_key=True, comment="结算期ID（settlement_periods.period_id）")
    user_id = Column(BigInteger, primary_key=True, comment="用户ID（本期被结算的用户）")

    gross_coins = Column(BigInteger, nullable=False, default=0, comment="本期统计区间内的总金币")
    self_keep_coins = Column(BigInteger, nullable=False, default=0, comment="号主自留金币")
    self_payable_coins = Column(BigInteger, nullable=False, default=0, comment="号主应缴金币")

    l1_user_id = Column(BigInteger, nullable=True, comment="本期快照下的一级上级（+1）user_id")
    l2_user_id = Column(BigInteger, nullable=True, comment="本期快照下的二级上级（+2）user_id")

    l1_commission_coins = Column(BigInteger, nullable=False, default=0, comment="+1 理论分成金币")
    l2_commission_coins = Column(BigInteger, nullable=False, default=0, comment="+2 理论分成金币")
    platform_retain_coins = Column(BigInteger, nullable=False, default=0, comment="平台理论留存金币")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_income_l1", "period_id", "l1_user_id"),
        Index("idx_income_l2", "period_id", "l2_user_id"),
    )

    def __repr__(self):
        return f"<SettlementUserIncome(period_id={self.period_id}, user_id={self.user_id}, gross_coins={self.gross_coins})>"


class SettlementUserPayable(Base):
    """结算期应缴义务表：记录每期每用户应缴金额、累计已缴金额与状态"""
    __tablename__ = "settlement_user_payable"

    period_id = Column(BigInteger, primary_key=True, comment="结算期ID（settlement_periods.period_id）")
    user_id = Column(BigInteger, primary_key=True, comment="用户ID（本期应缴义务所属用户）")

    amount_due_coins = Column(BigInteger, nullable=False, default=0, comment="本期应缴金币")
    amount_paid_coins = Column(BigInteger, nullable=False, default=0, comment="本期累计已缴金币")

    status = Column(Integer, nullable=False, default=0, comment="缴费状态：0=UNPAID 1=PARTIAL 2=PAID 3=OVERDUE")
    first_paid_at = Column(DateTime(timezone=True), nullable=True, comment="首次产生有效缴费的时间")
    paid_at = Column(DateTime(timezone=True), nullable=True, comment="缴清时间")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_payable_status", "period_id", "status"),
    )

    def __repr__(self):
        return f"<SettlementUserPayable(period_id={self.period_id}, user_id={self.user_id}, due={self.amount_due_coins}, paid={self.amount_paid_coins})>"


class SettlementPayment(Base):
    """缴费记录表：记录每期用户提交的每一笔缴费凭证及审核结果"""
    __tablename__ = "settlement_payments"

    payment_id = Column(BigInteger, primary_key=True, index=True, autoincrement=True, comment="缴费记录ID（主键，自增）")
    period_id = Column(BigInteger, nullable=False, comment="结算期ID（settlement_periods.period_id）")
    payer_user_id = Column(BigInteger, nullable=False, comment="缴费人用户ID（本笔缴费由谁提交）")

    amount_coins = Column(BigInteger, nullable=False, comment="本次缴费金额（coins，整数）")
    method = Column(String(20), nullable=False, default="manual", comment="缴费方式：manual/alipay/wechat/bank/...")
    proof_url = Column(String(512), nullable=True, comment="缴费凭证URL")

    status = Column(Integer, nullable=False, default=0, comment="审核状态：0=SUBMITTED 1=CONFIRMED 2=REJECTED")

    submitted_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="提交时间")
    confirmed_at = Column(DateTime(timezone=True), nullable=True, comment="确认时间")
    confirmed_by = Column(BigInteger, nullable=True, comment="确认人（管理员user_id）")
    reject_reason = Column(String(255), nullable=True, comment="驳回原因")

    __table_args__ = (
        Index("idx_payments_period_user", "period_id", "payer_user_id"),
        Index("idx_payments_status", "period_id", "status"),
    )

    def __repr__(self):
        return f"<SettlementPayment(payment_id={self.payment_id}, period_id={self.period_id}, payer_user_id={self.payer_user_id}, status={self.status})>"


class SettlementCommission(Base):
    """分成明细表：按期记录来源用户对上级的分成金额（资金化/解锁状态可追溯）"""
    __tablename__ = "settlement_commissions"

    period_id = Column(BigInteger, primary_key=True)
    source_user_id = Column(BigInteger, primary_key=True, comment="谁产生收益")
    beneficiary_user_id = Column(BigInteger, primary_key=True, comment="谁拿分成（上级）")
    level = Column(Integer, primary_key=True, comment="1 或 2")
    amount_coins = Column(BigInteger, nullable=False, comment="分成金额（coins）")

    funding_status = Column(Integer, nullable=False, default=0, comment="0=UNFUNDED(来源未缴费),1=FUNDED")
    funded_at = Column(DateTime(timezone=True), nullable=True)

    is_unlocked = Column(Integer, nullable=False, default=0, comment="0=锁定,1=已解锁")
    unlocked_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_comm_beneficiary", "period_id", "beneficiary_user_id", "funding_status", "is_unlocked"),
        Index("idx_comm_source", "period_id", "source_user_id", "funding_status"),
    )

    def __repr__(self):
        return (
            f"<SettlementCommission(period_id={self.period_id}, source_user_id={self.source_user_id}, "
            f"beneficiary_user_id={self.beneficiary_user_id}, level={self.level}, amount_coins={self.amount_coins})>"
        )


class WalletAccount(Base):
    """钱包账户表（coins 账本）"""
    __tablename__ = "wallet_accounts"

    user_id = Column(BigInteger, ForeignKey("users.id"), primary_key=True)
    available_coins = Column(BigInteger, nullable=False, default=0, comment="可用余额（coins）")
    locked_coins = Column(BigInteger, nullable=False, default=0, comment="锁定余额（coins）")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # 关系
    user = relationship("User", back_populates="wallet")

    def __repr__(self):
        return f"<WalletAccount(user_id={self.user_id}, available_coins={self.available_coins}, locked_coins={self.locked_coins})>"


class WalletLedger(Base):
    """钱包账本流水（coins 维度）"""
    __tablename__ = "wallet_ledger"

    ledger_id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    period_id = Column(BigInteger, nullable=True)

    entry_type = Column(String(40), nullable=False, comment="COMMISSION_LOCKED_IN / COMMISSION_UNLOCK / WITHDRAW_* / ADJUST")
    delta_available_coins = Column(BigInteger, nullable=False, default=0)
    delta_locked_coins = Column(BigInteger, nullable=False, default=0)

    ref_source_user_id = Column(BigInteger, nullable=True, comment="关联来源下级 user_id")
    remark = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_ledger_user_time", "user_id", "created_at"),
        Index("idx_ledger_user_period", "user_id", "period_id"),
    )

    def __repr__(self):
        return f"<WalletLedger(ledger_id={self.ledger_id}, user_id={self.user_id}, entry_type={self.entry_type})>"


class WithdrawRequest(Base):
    """提现申请表：提现可追踪、可审核、可回滚（余额变更必须走账本）"""
    __tablename__ = "withdraw_requests"

    withdraw_id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    amount_coins = Column(BigInteger, nullable=False)

    method = Column(String(20), nullable=False, default="manual")
    account_info = Column(String(255), nullable=True)

    status = Column(Integer, nullable=False, default=0, comment="0=PENDING,1=APPROVED,2=PAID,3=REJECTED,4=CANCELED")
    requested_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    processed_at = Column(DateTime(timezone=True), nullable=True)
    processed_by = Column(BigInteger, nullable=True)
    reject_reason = Column(String(255), nullable=True)

    __table_args__ = (
        Index("idx_withdraw_user_status", "user_id", "status"),
        Index("idx_withdraw_time", "requested_at"),
    )

    user = relationship("User")

    def __repr__(self):
        return f"<WithdrawRequest(withdraw_id={self.withdraw_id}, user_id={self.user_id}, status={self.status})>"


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
