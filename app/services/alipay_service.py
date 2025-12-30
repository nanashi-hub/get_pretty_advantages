"""
支付宝集成服务
提供订��查询、转账等功能
"""
import os
import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict
from urllib.parse import quote_plus

import requests

from sqlalchemy.orm import Session
from app.database import get_db
from app.models import (
    AlipayConfig, RechargeOrder, TransferRecord,
    RechargeOrderStatus, TransferStatus,
    User, UserReferral, WalletAccount, WalletTransaction, TransactionType
)


class AlipayClient:
    """支付宝 API 客户端"""

    def __init__(self, config: AlipayConfig):
        self.config = config
        self.app_id = config.app_id
        self.private_key = config.private_key
        self.alipay_public_key = config.alipay_public_key
        self.gateway = config.gateway
        self.sign_type = config.sign_type

    def _build_params(self, biz_content: dict) -> dict:
        """构建请求参数"""
        params = {
            "app_id": self.app_id,
            "method": "",
            "charset": "utf-8",
            "sign_type": self.sign_type,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "version": "1.0",
            "biz_content": json.dumps(biz_content, ensure_ascii=False)
        }
        return params

    def _sign(self, params: dict) -> str:
        """生成签名"""
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.backends import default_backend

        # 过滤空值和sign
        filtered = {k: v for k, v in params.items() if v and k != "sign"}
        # 按字典序排序
        sorted_params = sorted(filtered.items())
        # 拼接字符串
        sign_str = "&".join([f"{k}={v}" for k, v in sorted_params])

        # 加载私钥
        private_key = serialization.load_pem_private_key(
            self.private_key.encode(),
            password=None,
            backend=default_backend()
        )

        # 签名
        signature = private_key.sign(
            sign_str.encode("utf-8"),
            algorithm=hashes.SHA256() if self.sign_type == "RSA2" else hashes.SHA1()
        )

        import base64
        return base64.b64encode(signature).decode("utf-8")

    def _verify_sign(self, params: dict) -> bool:
        """验证签名"""
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.backends import default_backend

        sign = params.pop("sign", "")
        sign_bytes = base64.b64decode(sign)

        # 过滤空值和sign
        filtered = {k: v for k, v in params.items() if v}
        sorted_params = sorted(filtered.items())
        sign_str = "&".join([f"{k}={v}" for k, v in sorted_params])

        # 加载公钥
        public_key = serialization.load_pem_public_key(
            self.alipay_public_key.encode(),
            backend=default_backend()
        )

        try:
            if self.sign_type == "RSA2":
                public_key.verify(sign_bytes, sign_str.encode("utf-8"), hashes.SHA256())
            else:
                public_key.verify(sign_bytes, sign_str.encode("utf-8"), hashes.SHA1())
            return True
        except Exception:
            return False

    def _request(self, method: str, biz_content: dict, notify_url: str = None) -> dict:
        """发送请求"""
        params = self._build_params(biz_content)
        params["method"] = method
        if notify_url:
            params["notify_url"] = notify_url

        # 生成签名
        params["sign"] = self._sign(params)

        # 发送请求
        response = requests.post(self.gateway, data=params, timeout=30)
        response.raise_for_status()

        result = response.json()

        # 验证签名
        if "sign" in result and not self._verify_sign(result.copy()):
            raise ValueError("支付宝签名验证失败")

        return result

    def query_order(self, out_trade_no: str) -> dict:
        """查询订单状态"""
        try:
            result = self._request(
                "alipay.trade.query",
                {"out_trade_no": out_trade_no}
            )
            return result
        except Exception as e:
            return {"error": str(e)}

    def query_bill(self, start_date: str, end_date: str) -> dict:
        """查询账单（下载对账单）"""
        try:
            result = self._request(
                "alipay.data.dataservice.bill.downloadurl.query",
                {
                    "bill_type": "trade",
                    "bill_date": start_date,  # 格式: yyyy-MM-dd
                    "start_time": start_date,
                    "end_time": end_date
                }
            )
            return result
        except Exception as e:
            return {"error": str(e)}

    def transfer(self, out_biz_no: str, payee_account: str, amount: str,
                 payee_real_name: str = None, remark: str = "") -> dict:
        """单笔转账"""
        biz_content = {
            "out_biz_no": out_biz_no,
            "payee_type": "ALIPAY_LOGON_ID",  # 支付宝登录号
            "payee_account": payee_account,
            "amount": amount,
            "remark": remark
        }
        if payee_real_name:
            biz_content["payee_real_name"] = payee_real_name

        try:
            result = self._request(
                "alipay.fund.trans.uni.transfer",
                biz_content
            )
            return result
        except Exception as e:
            return {"error": str(e)}

    def transfer_query(self, out_biz_no: str) -> dict:
        """查询转账"""
        try:
            result = self._request(
                "alipay.fund.trans.common.query",
                {
                    "out_biz_no": out_biz_no,
                    "product_code": "TRANS_ACCOUNT_NO_PWD"
                }
            )
            return result
        except Exception as e:
            return {"error": str(e)}


def get_alipay_config(db: Session) -> Optional[AlipayConfig]:
    """获取启用的支付宝配置"""
    return db.query(AlipayConfig).filter(
        AlipayConfig.status == 1
    ).order_by(AlipayConfig.id.desc()).first()


def generate_order_no() -> str:
    """生成订单号 CZ + 年月日时分秒 + 4位随机数"""
    now = datetime.now()
    random_part = os.urandom(2).hex()
    return f"CZ{now.strftime('%Y%m%d%H%M%S')}{random_part}".upper()


def calculate_settlement(amount: Decimal, user_id: int, db: Session) -> List[Dict]:
    """
    计算分账金额
    返回分账列表: [{"role": "platform", "user_id": None, "amount": xxx}, ...]
    """
    config = get_alipay_config(db)
    if not config:
        raise ValueError("支付宝配置未设置")

    # 获取推广关系
    referral = db.query(UserReferral).filter(
        UserReferral.user_id == user_id
    ).first()

    settlements = []

    # 平台抽成
    platform_fee = amount * config.platform_fee_rate
    settlements.append({
        "role": "platform",
        "user_id": None,
        "amount": platform_fee,
        "alipay_account": config.alipay_account or None
    })

    # 剩余金额用于分配
    remaining = amount - platform_fee

    if referral:
        # 有上级：按比例分配
        agent_l1_id = referral.inviter_level1
        agent_l2_id = referral.inviter_level2

        # 一级代理
        if agent_l1_id:
            l1_amount = remaining * config.agent_l1_rate
            settlements.append({
                "role": "agent_l1",
                "user_id": agent_l1_id,
                "amount": l1_amount
            })
            remaining -= l1_amount

        # 二级代理
        if agent_l2_id:
            l2_amount = remaining * config.agent_l2_rate
            settlements.append({
                "role": "agent_l2",
                "user_id": agent_l2_id,
                "amount": l2_amount
            })
            remaining -= l2_amount

        # 号主得剩余部分
        settlements.append({
            "role": "user",
            "user_id": user_id,
            "amount": remaining
        })
    else:
        # 无上级：全部给用户
        settlements.append({
            "role": "user",
            "user_id": user_id,
            "amount": remaining
        })

    return settlements


def distribute_amount(recharge_order: RechargeOrder, db: Session) -> List[TransferRecord]:
    """
    执行分账：计算并创建转账记录
    """
    if recharge_order.status != RechargeOrderStatus.PAID:
        raise ValueError("订单未支付，无法分账")

    # 检查是否已分账
    existing = db.query(TransferRecord).filter(
        TransferRecord.recharge_order_id == recharge_order.id
    ).first()
    if existing:
        raise ValueError("订单已分账")

    # 计算分账
    settlements = calculate_settlement(recharge_order.amount, recharge_order.user_id, db)
    alipay_config = get_alipay_config(db)

    transfer_records = []

    for settlement in settlements:
        # 获取收款人支付宝账号
        if settlement["role"] == "platform":
            alipay_account = settlement.get("alipay_account")
        else:
            user = db.query(User).filter(User.id == settlement["user_id"]).first()
            alipay_account = user.alipay_account if user else None

        transfer = TransferRecord(
            recharge_order_id=recharge_order.id,
            user_id=settlement["user_id"],
            amount=settlement["amount"],
            role=settlement["role"],
            alipay_account=alipay_account or "未设置",
            status=TransferStatus.PENDING
        )
        db.add(transfer)
        transfer_records.append(transfer)

    db.commit()

    # 尝试执行转账
    _execute_transfers(transfer_records, alipay_config, db)

    return transfer_records


def _execute_transfers(transfers: List[TransferRecord], alipay_config: AlipayConfig, db: Session):
    """执行转账操作"""
    client = AlipayClient(alipay_config)

    for transfer in transfers:
        if not transfer.alipay_account or transfer.alipay_account == "未设置":
            transfer.status = TransferStatus.FAILED
            transfer.fail_reason = "收款账号未设置"
            db.commit()
            continue

        out_biz_no = f"TX{transfer.id}{datetime.now().strftime('%Y%m%d%H%M%S')}"

        try:
            result = client.transfer(
                out_biz_no=out_biz_no,
                payee_account=transfer.alipay_account,
                amount=str(transfer.amount),
                remark=f"分账-{transfer.role}"
            )

            if result.get("error"):
                transfer.status = TransferStatus.FAILED
                transfer.fail_reason = result["error"]
            elif result.get("code") == "10000":
                # 成功
                transfer.alipay_order_id = result.get("order_id")
                transfer.alipay_status = result.get("status")
                transfer.status = TransferStatus.SUCCESS
                transfer.transferred_at = datetime.now()
            else:
                # 失败
                transfer.status = TransferStatus.FAILED
                transfer.fail_reason = result.get("sub_msg", result.get("msg", "未知错误"))

            db.commit()

        except Exception as e:
            transfer.status = TransferStatus.FAILED
            transfer.fail_reason = str(e)
            db.commit()


def check_pending_payments(db: Session) -> dict:
    """
    检查待支付订单

    注意：当前使用的是个人收款码转账场景，用户在付款备注中填写订单号。
    由于支付宝开放API限制，无法直接查询个人收款码的转账记录。

    建议方案：
    1. 使用支付宝商户版的交易查询接口（需要签约当面付或手机网站支付）
    2. 或采用人工审核方式：管理员查看支付宝账单后手动确认
    3. 或接入支付宝资金账单下载接口 alipay.data.dataservice.bill.downloadurl.query

    当前实现：返回待处理订单列表，供管理员手动确认
    """
    alipay_config = get_alipay_config(db)
    if not alipay_config:
        return {"error": "支付宝配置未设置"}

    client = AlipayClient(alipay_config)

    # 获取待支付订单（未过期）
    pending_orders = db.query(RechargeOrder).filter(
        RechargeOrder.status == RechargeOrderStatus.PENDING,
        RechargeOrder.expired_at > datetime.now()
    ).all()

    confirmed_count = 0
    for order in pending_orders:
        # 尝试查询支付宝订单（仅对通过支付宝接口创建的订单有效）
        result = client.query_order(order.order_no)

        if result.get("error"):
            # 查询失败，可能是个人收款码场景，跳过
            continue

        # 检查响应结构
        response_key = f"{alipay_config.gateway.replace('https://openapi.alipay.com/gateway.do', '').replace('/gateway.do', '')}_response"
        response = result.get("response") or result.get(response_key, {})

        if response.get("code") == "10000":
            trade_status = response.get("trade_status")
            if trade_status in ["TRADE_SUCCESS", "TRADE_FINISHED"]:
                # 交易成功
                order.alipay_trade_no = response.get("trade_no")
                order.alipay_log_id = response.get("out_trade_no")
                order.status = RechargeOrderStatus.PAID
                order.paid_at = datetime.now()
                db.commit()

                # 自动分账
                try:
                    distribute_amount(order, db)
                    order.status = RechargeOrderStatus.CONFIRMED
                    order.confirmed_at = datetime.now()
                    confirmed_count += 1
                except Exception as e:
                    # 分账失败，订单状态保持为PAID，需要手动处理
                    print(f"分账失败: {e}")

                db.commit()

    return {
        "checked_orders": len(pending_orders),
        "confirmed_orders": confirmed_count,
        "message": f"已检查 {len(pending_orders)} 个待支付订单，确认 {confirmed_count} 个"
    }


def manually_confirm_payment(order_no: str, alipay_trade_no: str, db: Session) -> dict:
    """
    手动确认支付

    当用户通过个人收款码转账后，管理员可以在支付宝账单中看到交易记录，
    然后使用此接口手动确认支付。

    Args:
        order_no: 订单号
        alipay_trade_no: 支付宝交易号（从支付宝账单中获取）
        db: 数据库会话
    """
    order = db.query(RechargeOrder).filter(
        RechargeOrder.order_no == order_no
    ).first()

    if not order:
        raise ValueError("订单不存在")

    if order.status != RechargeOrderStatus.PENDING:
        raise ValueError(f"订单状态为 {order.status.value}，无法确认")

    # 更新订单状态
    order.alipay_trade_no = alipay_trade_no
    order.status = RechargeOrderStatus.PAID
    order.paid_at = datetime.now()
    db.commit()

    # 执行分账
    try:
        transfers = distribute_amount(order, db)
        order.status = RechargeOrderStatus.CONFIRMED
        order.confirmed_at = datetime.now()
        db.commit()

        return {
            "success": True,
            "order_no": order_no,
            "transfers_count": len(transfers),
            "message": "支付确认成功，分账完成"
        }
    except Exception as e:
        # 分账失败
        return {
            "success": False,
            "order_no": order_no,
            "error": str(e),
            "message": "支付已确认，但分账失败，请手动处理"
        }


def get_wallet_with_alipay(user_id: int, db: Session) -> dict:
    """获取用户钱包和支付宝信息"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return None

    wallet = db.query(WalletAccount).filter(WalletAccount.user_id == user_id).first()
    if not wallet:
        wallet = WalletAccount(user_id=user_id, balance=0)
        db.add(wallet)
        db.commit()
        db.refresh(wallet)

    return {
        "user_id": user.id,
        "username": user.username,
        "nickname": user.nickname,
        "balance": float(wallet.balance),
        "alipay_account": user.alipay_account
    }
