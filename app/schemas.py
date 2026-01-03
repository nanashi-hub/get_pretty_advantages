from pydantic import BaseModel, Field, validator
from datetime import datetime, date
from typing import Optional, List
from decimal import Decimal
from app.models import (
    UserRole, ConfigStatus, EnvStatus, KSAccountStatus,
    RunLogStatus, TransactionType
)


# ==================== 用户相关 ====================

class UserRegister(BaseModel):
    """用户注册数据模型"""
    username: str = Field(..., min_length=3, max_length=50, description="用户名/登录名")
    password: str = Field(..., min_length=6, max_length=72, description="密码（最多72字节）")
    nickname: Optional[str] = Field(None, max_length=50, description="昵称/备注名")
    phone: Optional[str] = Field(None, max_length=20, description="手机号")
    wechat_id: Optional[str] = Field(None, max_length=50, description="微信ID（用于联系/结算）")
    invite_code: Optional[str] = Field(None, description="邀请码（可选，用于建立推广关系）")

    @validator('phone')
    def validate_phone(cls, v):
        if v and not v.isdigit():
            raise ValueError('手机号只能包含数字')
        return v
    
    @validator('password')
    def validate_password_length(cls, v):
        """验证密码字节长度不超过72字节（bcrypt限制）"""
        if isinstance(v, str):
            password_bytes = v.encode('utf-8')
            if len(password_bytes) > 72:
                raise ValueError('密码长度超过限制（最多72字节，约54个字符）')
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "username": "testuser",
                "password": "password123",
                "nickname": "测试用户",
                "phone": "13800138000",
                "wechat_id": "wxid_test",
                "invite_code": "INVITE123"
            }
        }


class UserLogin(BaseModel):
    """用户登录数据模型"""
    username_or_email: str = Field(..., description="用户名或手机号")
    password: str = Field(..., description="密码")

    class Config:
        json_schema_extra = {
            "example": {
                "username_or_email": "testuser",
                "password": "password123"
            }
        }


class UserResponse(BaseModel):
    """用户信息响应模型"""
    id: int
    username: str
    nickname: Optional[str] = None
    phone: Optional[str] = None
    wechat_id: Optional[str] = None
    role: str
    status: int
    referral_code: Optional[str] = None  # 我的推广码
    inviter_id: Optional[int] = None     # 直接邀请人ID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class BindInviterRequest(BaseModel):
    """绑定邀请人请求"""
    invite_code: str = Field(..., description="邀请码/推广码")


class ReferralInfo(BaseModel):
    """推广信息响应"""
    my_referral_code: str  # 我的推广码
    inviter: Optional[dict] = None  # 我的邀请人信息
    level1_count: int = 0  # 我直接邀请的人数
    level2_count: int = 0  # 我间接邀请的人数（+2）


class UserUpdate(BaseModel):
    """用户更新数据模型"""
    nickname: Optional[str] = Field(None, max_length=50)
    phone: Optional[str] = Field(None, max_length=20)
    wechat_id: Optional[str] = Field(None, max_length=50)
    role: Optional[str] = None
    status: Optional[int] = None


class AccountUpdate(BaseModel):
    """个人账户更新"""
    username: Optional[str] = Field(None, min_length=3, max_length=50)
    nickname: Optional[str] = Field(None, max_length=50)
    phone: Optional[str] = Field(None, max_length=20)
    wechat_id: Optional[str] = Field(None, max_length=50)


class PasswordUpdate(BaseModel):
    """修改密码"""
    new_password: str = Field(..., min_length=6, max_length=72)


class Token(BaseModel):
    """Token响应模型"""
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class Message(BaseModel):
    """消息响应模型"""
    message: str


# ==================== 青龙实例相关 ====================

class QLInstanceCreate(BaseModel):
    """创建青龙实例"""
    name: str = Field(..., max_length=50, description="青龙实例名称")
    base_url: str = Field(..., max_length=255, description="http://ip:5700")
    client_id: str = Field(..., max_length=100, description="青龙应用 client_id")
    client_secret: str = Field(..., max_length=100, description="青龙应用 client_secret")
    remark: Optional[str] = Field(None, max_length=255, description="备注")
    status: int = Field(1, description="1=可用,0=停用")


class QLInstanceUpdate(BaseModel):
    """更新青龙实例"""
    name: Optional[str] = Field(None, max_length=50)
    base_url: Optional[str] = Field(None, max_length=255)
    client_id: Optional[str] = Field(None, max_length=100)
    client_secret: Optional[str] = Field(None, max_length=100)
    remark: Optional[str] = Field(None, max_length=255)
    status: Optional[int] = None


class QLInstanceResponse(BaseModel):
    """青龙实例响应"""
    id: int
    name: str
    base_url: str
    client_id: str
    client_secret: str
    remark: Optional[str] = None
    status: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ==================== 脚本配置相关 ====================

class UserScriptConfigCreate(BaseModel):
    """创建用户脚本配置"""
    user_id: Optional[int] = Field(None, description="归属用户ID（管理员可指定，不填默认当前用户）")
    ql_instance_id: int = Field(..., description="对应青龙实例ID")
    script_name: str = Field(..., max_length=100, description="脚本名称")
    group_key: str = Field(..., max_length=100, description="配置组key")
    status: str = Field("enabled", description="状态: enabled/disabled")


class UserScriptConfigUpdate(BaseModel):
    """更新用户脚本配置"""
    script_name: Optional[str] = Field(None, max_length=100)
    group_key: Optional[str] = Field(None, max_length=100)
    status: Optional[str] = None


class UserScriptConfigResponse(BaseModel):
    """用户脚本配置响应"""
    id: int
    user_id: int
    ql_instance_id: int
    script_name: str
    group_key: str
    status: str
    last_sync_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ==================== 环境变量相关 ====================

class UserScriptEnvCreate(BaseModel):
    """创建环境变量"""
    config_id: int = Field(..., description="配置ID")
    env_name: str = Field(..., max_length=100, description="环境变量名")
    env_value: str = Field(..., description="变量值")
    ql_env_id: Optional[str] = Field(None, max_length=100)
    ip_id: Optional[int] = Field(None, description="IP池ID")
    status: str = Field("valid", description="状态: valid/invalid")
    remark: Optional[str] = Field(None, max_length=255)


class UserScriptEnvUpdate(BaseModel):
    """更新环境变量"""
    env_name: Optional[str] = Field(None, max_length=100)
    env_value: Optional[str] = None
    ql_env_id: Optional[str] = Field(None, max_length=100)
    ip_id: Optional[int] = Field(None, description="IP池ID")
    status: Optional[str] = None
    remark: Optional[str] = Field(None, max_length=255)


class IPInfo(BaseModel):
    """IP池信息"""
    id: int
    proxy_url: Optional[str] = None
    region: Optional[str] = None
    vendor: Optional[str] = None
    max_users: int
    used: int = 0


class UserScriptEnvResponse(BaseModel):
    """环境变量响应"""
    id: int
    config_id: int
    env_name: str
    env_value: str
    ql_env_id: Optional[str] = None
    ip_id: Optional[int] = None
    ip_info: Optional[IPInfo] = None
    status: str
    remark: Optional[str] = None
    disabled_until: Optional[datetime] = None
    disable_days: Optional[int] = None
    disabled_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class EnvDisableRequest(BaseModel):
    """禁用环境变量请求"""
    days: int = Field(..., ge=1, le=30, description="禁用天数（1-30天），支持3/5/7天")

    class Config:
        json_schema_extra = {
            "example": {
                "days": 3
            }
        }


# ==================== 快手账号相关 ====================

class KSAccountCreate(BaseModel):
    """创建快手账号"""
    user_id: int = Field(..., description="号主用户ID")
    config_id: Optional[int] = Field(None, description="关联配置ID")
    mobile: Optional[str] = Field(None, max_length=20, description="快手绑定手机号")
    ks_uid: Optional[str] = Field(None, max_length=50, description="快手用户ID")
    current_ck: Optional[str] = Field(None, description="当前CK")
    status: str = Field("normal", description="状态: normal/black/banned/expired")
    device_info: Optional[str] = Field(None, max_length=255)
    ip_group: Optional[str] = Field(None, max_length=50)


class KSAccountUpdate(BaseModel):
    """更新快手账号"""
    config_id: Optional[int] = None
    mobile: Optional[str] = Field(None, max_length=20)
    ks_uid: Optional[str] = Field(None, max_length=50)
    current_ck: Optional[str] = None
    status: Optional[str] = None
    device_info: Optional[str] = Field(None, max_length=255)
    ip_group: Optional[str] = Field(None, max_length=50)


class KSAccountResponse(BaseModel):
    """快手账号响应"""
    id: int
    user_id: int
    config_id: Optional[int] = None
    mobile: Optional[str] = None
    ks_uid: Optional[str] = None
    current_ck: Optional[str] = None
    status: str
    last_ck_refresh_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None
    device_info: Optional[str] = None
    ip_group: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ==================== 脚本运行开关相关 ====================

class ScriptRunSwitchCreate(BaseModel):
    """创建运行开关"""
    config_id: int = Field(..., description="配置ID")
    is_enabled: int = Field(1, description="1=开，0=关")
    cron_expr: Optional[str] = Field(None, max_length=100)
    max_daily_runs: Optional[int] = None


class ScriptRunSwitchUpdate(BaseModel):
    """更新运行开关"""
    is_enabled: Optional[int] = None
    cron_expr: Optional[str] = Field(None, max_length=100)
    max_daily_runs: Optional[int] = None


class ScriptRunSwitchResponse(BaseModel):
    """运行开关响应"""
    id: int
    config_id: int
    is_enabled: int
    cron_expr: Optional[str] = None
    max_daily_runs: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ==================== 运行日志相关 ====================

class ScriptRunLogCreate(BaseModel):
    """创建运行日志"""
    config_id: int
    ks_account_id: Optional[int] = None
    task_name: str = Field(..., max_length=100)
    run_at: datetime
    status: str = Field("success")
    coins_earned: int = Field(0)
    raw_log_snippet: Optional[str] = None


class ScriptRunLogResponse(BaseModel):
    """运行日志响应"""
    id: int
    config_id: int
    ks_account_id: Optional[int] = None
    task_name: str
    run_at: datetime
    status: str
    coins_earned: int
    raw_log_snippet: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


# ==================== 收益记录相关 ====================

class EarningRecordCreate(BaseModel):
    """创建收益记录"""
    env_id: int = Field(..., description="账号实体ID（user_script_envs.id）")
    stat_date: date = Field(..., description="统计日期（按天）")
    account_remark: Optional[str] = Field(None, max_length=255, description="当日统计口径账号标识（快照）")
    coins_total: int = 0
    coins_from_look: int = 0
    coins_from_lookk: int = 0
    coins_from_dj: int = 0
    coins_from_food: int = 0
    coins_from_box: int = 0
    coins_from_search: int = 0
    record_note: Optional[str] = Field(None, max_length=255, description="记录备注（调试/回填说明/来源等）")


class EarningRecordUpdate(BaseModel):
    """更新收益记录"""
    coins_total: Optional[int] = None
    coins_from_look: Optional[int] = None
    coins_from_lookk: Optional[int] = None
    coins_from_dj: Optional[int] = None
    coins_from_food: Optional[int] = None
    coins_from_box: Optional[int] = None
    coins_from_search: Optional[int] = None
    record_note: Optional[str] = Field(None, max_length=255)


class EarningRecordResponse(BaseModel):
    """收益记录响应"""
    env_id: int
    user_id: Optional[int] = None
    stat_date: date
    account_remark: str
    coins_total: int
    coins_from_look: int
    coins_from_lookk: int
    coins_from_dj: int
    coins_from_food: int
    coins_from_box: int
    coins_from_search: int
    record_note: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ==================== 结算（阶段1）相关 ====================

class SettlementPeriodCreate(BaseModel):
    """创建结算期（管理员）"""
    period_start: date
    period_end: date
    pay_start: date
    pay_end: date

    coin_rate: int = Field(10000, ge=1, description="coin_rate coins = 1 元")
    host_bps: int = Field(6000, ge=0, le=10000)
    l1_bps: int = Field(2000, ge=0, le=10000)
    l2_bps: int = Field(400, ge=0, le=10000)
    collect_bps: int = Field(4000, ge=0, le=10000)

    status: int = Field(0, ge=0, le=2, description="0=OPEN 1=PAYING 2=CLOSED")


class SettlementPeriodResponse(BaseModel):
    """结算期响应"""
    period_id: int
    period_start: date
    period_end: date
    pay_start: date
    pay_end: date
    coin_rate: int
    host_bps: int
    l1_bps: int
    l2_bps: int
    collect_bps: int
    status: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class SettlementUserIncomeResponse(BaseModel):
    """结算期用户收益汇总响应"""
    period_id: int
    user_id: int
    gross_coins: int
    self_keep_coins: int
    self_payable_coins: int
    l1_user_id: Optional[int] = None
    l2_user_id: Optional[int] = None
    l1_commission_coins: int
    l2_commission_coins: int
    platform_retain_coins: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class SettlementUserPayableResponse(BaseModel):
    """结算期应缴义务响应"""
    period_id: int
    user_id: int
    amount_due_coins: int
    amount_paid_coins: int
    status: int
    first_paid_at: Optional[datetime] = None
    paid_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class SettlementPaymentCreate(BaseModel):
    """提交缴费（用户）"""
    period_id: Optional[int] = Field(None, description="为空则提交到当前结算期")
    amount_coins: int = Field(..., ge=1, description="本次缴费金额（coins，整数）")
    method: str = Field("manual", max_length=20)
    proof_url: Optional[str] = Field(None, max_length=512)


class SettlementPaymentReject(BaseModel):
    """驳回缴费（管理员）"""
    reject_reason: str = Field(..., max_length=255)


class SettlementPaymentResponse(BaseModel):
    """缴费记录响应"""
    payment_id: int
    period_id: int
    payer_user_id: int
    amount_coins: int
    method: str
    proof_url: Optional[str] = None
    status: int
    submitted_at: datetime
    confirmed_at: Optional[datetime] = None
    confirmed_by: Optional[int] = None
    reject_reason: Optional[str] = None

    class Config:
        from_attributes = True


class SettlementMeResponse(BaseModel):
    """结算中心（用户视角）聚合响应"""
    period: Optional[SettlementPeriodResponse] = None
    income: Optional[SettlementUserIncomeResponse] = None
    payable: Optional[SettlementUserPayableResponse] = None
    payments: List[SettlementPaymentResponse] = []
    alipay_qrcode_url: Optional[str] = None


# ==================== 钱包相关 ====================

class WalletAccountResponse(BaseModel):
    """钱包账户响应"""
    user_id: int
    available_coins: int
    locked_coins: int
    updated_at: datetime

    class Config:
        from_attributes = True


class WalletLedgerEntryResponse(BaseModel):
    """钱包账本流水响应"""
    ledger_id: int
    user_id: int
    period_id: Optional[int] = None
    entry_type: str
    delta_available_coins: int
    delta_locked_coins: int
    ref_source_user_id: Optional[int] = None
    remark: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class WalletDownlineDueSummary(BaseModel):
    """下级待缴汇总"""
    cnt: int = 0
    sum_due_coins: int = 0


class WalletSummaryResponse(BaseModel):
    """我的钱包汇总（用于钱包页）"""
    coin_rate: int = 10000
    wallet: WalletAccountResponse
    period: Optional[SettlementPeriodResponse] = None
    my_payable: Optional[SettlementUserPayableResponse] = None
    my_remaining_due_coins: int = 0
    l1_due: WalletDownlineDueSummary = WalletDownlineDueSummary()
    l2_due: WalletDownlineDueSummary = WalletDownlineDueSummary()
    commission_expected_coins: int = 0
    commission_funded_locked_coins: int = 0
    commission_unfunded_coins: int = 0


# ==================== 提现相关 ====================

class WithdrawRequestCreate(BaseModel):
    """提现申请"""
    amount_coins: int = Field(..., gt=0, description="提现金额（coins）")
    method: str = Field("manual", max_length=20, description="提现方式：manual/alipay/wechat/bank/...")
    account_info: Optional[str] = Field(None, max_length=255, description="收款信息（建议脱敏存储）")


class WithdrawRequestReject(BaseModel):
    """提现驳回原因"""
    reject_reason: str = Field(..., min_length=1, max_length=255)


class WithdrawRequestResponse(BaseModel):
    """提现申请响应"""
    withdraw_id: int
    user_id: int
    amount_coins: int
    method: str
    account_info: Optional[str] = None
    status: int
    requested_at: datetime
    processed_at: Optional[datetime] = None
    processed_by: Optional[int] = None
    reject_reason: Optional[str] = None

    class Config:
        from_attributes = True


# ==================== 统计相关 ====================

class DashboardStats(BaseModel):
    """仪表板统计数据"""
    total_users: int = 0
    total_ks_accounts: int = 0
    total_configs: int = 0
    total_ql_instances: int = 0
    yesterday_coins: int = 0
    week_coins: int = 0
    pending_settlements: int = 0
    wallet_balance: float = 0.0


class EarningStats(BaseModel):
    """收益统计"""
    date: date
    coins_total: int
    coins_from_food: int
    coins_from_look: int
    coins_from_box: int
    coins_from_search: int
