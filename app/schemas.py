from pydantic import BaseModel, Field, validator
from datetime import datetime, date
from typing import Optional, List
from decimal import Decimal
from app.models import (
    UserRole, ConfigStatus, EnvStatus, KSAccountStatus,
    RunLogStatus, SettlementPeriodStatus, SettlementStatus, TransactionType
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
    ks_account_id: int
    stat_date: date
    coins_total: int = 0
    coins_from_food: int = 0
    coins_from_look: int = 0
    coins_from_box: int = 0
    coins_from_search: int = 0
    remark: Optional[str] = Field(None, max_length=255)


class EarningRecordUpdate(BaseModel):
    """更新收益记录"""
    coins_total: Optional[int] = None
    coins_from_food: Optional[int] = None
    coins_from_look: Optional[int] = None
    coins_from_box: Optional[int] = None
    coins_from_search: Optional[int] = None
    remark: Optional[str] = Field(None, max_length=255)


class EarningRecordResponse(BaseModel):
    """收益记录响应"""
    id: int
    ks_account_id: int
    stat_date: date
    coins_total: int
    coins_from_food: int
    coins_from_look: int
    coins_from_box: int
    coins_from_search: int
    remark: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ==================== 结算周期相关 ====================

class SettlementPeriodCreate(BaseModel):
    """创建结算周期"""
    period_label: str = Field(..., max_length=50, description="例如 2025W01")
    start_date: date
    end_date: date
    status: str = Field("open")


class SettlementPeriodUpdate(BaseModel):
    """更新结算周期"""
    period_label: Optional[str] = Field(None, max_length=50)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    status: Optional[str] = None


class SettlementPeriodResponse(BaseModel):
    """结算周期响应"""
    id: int
    period_label: str
    start_date: date
    end_date: date
    status: str
    created_at: datetime

    class Config:
        from_attributes = True


# ==================== 结算明细相关 ====================

class SettlementDetailCreate(BaseModel):
    """创建结算明细"""
    period_id: int
    user_id: int
    coins_total: int = 0
    rate_per_10k: float = 1.00
    amount_total: float = 0
    amount_to_user: float = 0
    amount_to_level1: float = 0
    amount_to_level2: float = 0
    status: str = Field("pending")
    remark: Optional[str] = Field(None, max_length=255)


class SettlementDetailUpdate(BaseModel):
    """更新结算明细"""
    coins_total: Optional[int] = None
    rate_per_10k: Optional[float] = None
    amount_total: Optional[float] = None
    amount_to_user: Optional[float] = None
    amount_to_level1: Optional[float] = None
    amount_to_level2: Optional[float] = None
    status: Optional[str] = None
    settled_at: Optional[datetime] = None
    remark: Optional[str] = Field(None, max_length=255)


class SettlementDetailResponse(BaseModel):
    """结算明细响应"""
    id: int
    period_id: int
    user_id: int
    coins_total: int
    rate_per_10k: float
    amount_total: float
    amount_to_user: float
    amount_to_level1: float
    amount_to_level2: float
    status: str
    settled_at: Optional[datetime] = None
    remark: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ==================== 钱包相关 ====================

class WalletAccountResponse(BaseModel):
    """钱包账户响应"""
    id: int
    user_id: int
    balance: float
    updated_at: datetime
    created_at: datetime

    class Config:
        from_attributes = True


class WalletTransactionCreate(BaseModel):
    """创建钱包交易"""
    user_id: int
    amount: float
    type: str
    ref_id: Optional[int] = None
    description: Optional[str] = Field(None, max_length=255)


class WalletTransactionResponse(BaseModel):
    """钱包交易响应"""
    id: int
    user_id: int
    wallet_id: Optional[int] = None
    amount: float
    type: str
    ref_id: Optional[int] = None
    description: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


# ==================== 统计相关 ====================

class DashboardStats(BaseModel):
    """仪表板统计数据"""
    total_users: int = 0
    total_ks_accounts: int = 0
    total_configs: int = 0
    total_ql_instances: int = 0
    today_coins: int = 0
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
