<div align="center">

# 快手极速版 - 自动刷广告收益平台

[![FastAPI](https://img.shields.io/badge/FastAPI-0.104-green)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-orange)](https://www.mysql.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**🚀 自动化脚本管理 | 💰 收益统计 | 👥 多级推广分成 | 🔐 完整结算体系**

</div>

---

## ✨ 项目简介

这是一个基于 **FastAPI + MySQL** 的快手极速版自动化脚本管理平台，帮助用户轻松管理多个快手账号，自动化完成刷金币任务，并提供完整的收益统计、推广分成和结算提现功能。

### 核心亮点

| 特性 | 说明 |
|------|------|
| **🤖 自动化脚本** | 集成青龙面板，自动运行快手极速版刷金币脚本 |
| **📱 一键配置环境** | Web 界面添加快手 Cookie，自动同步到青龙，无需手动操作 |
| **📊 收益自动统计** | 按账号/按日统计金币收益，生成可视化趋势图 |
| **👥 多级推广分成** | 支持二级推广体系（+1/+2），自动计算分成收益 |
| **💵 完整结算体系** | 结算期管理 → 分成钱包 → 提现审核，资金流转可追溯 |
| **🔐 安全可靠** | JWT 认证、权限分级、操作日志、余额回滚 |

---

## 🎯 主要功能

### 1. 快手脚本自动化

- **青龙面板集成**：统一管理多个青龙实例，自动同步环境变量
- **Cookie 管理**：Web 界面添加快手账号 Cookie，自动生成 `ksck1~ksck888` 序列
- **智能复用**：删除 Cookie 后编号自动回收复用，避免编号浪费
- **IP 池管理**：支持代理 IP 配置，自动控制每个 IP 的使用容量
- **状态监控**：实时显示账号状态，支持临时禁用/启用

### 2. 收益统计

- **日维度统计**：每天自动统计各账号金币收益
- **分项统计**：look/lookk/dj/food/box/search 等分项金币明细
- **层级收益**：统计"我"及"+1/+2"下级的总收益
- **趋势图表**：近 7 天/30 天金币收益趋势可视化
- **收益汇总**：本周期总收入 = 我 × 100% + 一级下级 × 20% + 二级下级 × 4%

### 3. 推广分成体系

```
┌─────────────┐
│   平台      │ ← 10% 充值分账
└─────────────┘
       │
┌─────────────┐
│  +2 代理    │ ← 27% 充值分账 / 4% 收益分成
└─────────────┘
       │
┌─────────────┐
│  +1 代理    │ ← 54% 充值分账 / 20% 收益分成
└─────────────┘
       │
┌─────────────┐
│  号主       │ ← 9% 充值分账 / 60% 收益自留 / 40% 平台应缴
└─────────────┘
```

- **二级推广**：支持 +1（直接邀请）和 +2（间接邀请）层级
- **邀请码绑定**：用户注册时填写邀请码建立推广关系
- **自动分成**：下级缴清后分成自动入账到上级钱包
- **关系快照**：每期结算时冻结推广关系，保证历史数据可复算

### 4. 结算与钱包

#### 阶段 1：结算期管理
- 定义统计区间、缴费窗口、分成比例
- 生成关系快照、收益汇总、应缴义务
- 用户提交缴费凭证，管理员审核

#### 阶段 2：分成钱包
- 分成明细记录（资金化入账，locked 状态）
- 下级缴清后自动解锁为可用余额
- 钱包账本流水，所有资金变动可追溯

#### 阶段 3：提现管理
- 用户申请提现可用余额
- 管理员审核/打款/驳回（驳回支持余额回滚）

### 5. 充值分账

- 支付宝收款码展示
- 支付成功后自动按比例分账
- 支持手动确认支付（个人收款码场景）
- 分账比例：平台 10% / +1 代理 54% / +2 代理 27% / 号主 9%

---

## 🚀 快速开始

### 方式一：Docker 部署（推荐）

1. **准备数据库**

```sql
CREATE DATABASE get_pretty_advantages DEFAULT CHARACTER SET utf8mb4;
```

2. **配置环境变量**（创建 `.env` 文件）

```bash
DATABASE_URL=mysql+pymysql://root:password@127.0.0.1:3306/get_pretty_advantages?charset=utf8mb4
ADMIN_SECRET=ADMIN_SECRET_KEY_2024
```

3. **启动服务**

```bash
docker compose up -d --build
```

4. **访问 Web**

- 地址：`http://localhost:1212`
- 首个注册用户自动成为管理员

### 方式二：本地开发

```bash
# 安装依赖
pip install -r requirements.txt

# 启动服务
uvicorn app.main:app --reload --port 1212
```

---

## 📖 使用指南

### 新手入门

1. **下载快手极速版 APK**
   - 仪表板首页提供 APK 下载链接
   - 建议使用指定版本以确保脚本正常运行

2. **配置青龙面板**
   - 管理员在「青龙实例」页面添加青龙面板
   - 系统会自动测试连接并获取 API Token

3. **添加快手账号**
   - 在「配置环境」页面添加快手 Cookie
   - 系统自动生成环境变量名（ksck1、ksck2...）
   - 自动同步到青龙面板

4. **查看收益**
   - 「收益统计」页面查看每日金币收益
   - 支持按账号/按用户维度查看趋势图

### 页面导航

| 页面 | 路由 | 说明 |
|------|------|------|
| 仪表板 | `/dashboard` | 新手指南、统计卡片、收益趋势、推广码 |
| 配置环境 | `/config-envs` | 添加/管理快手 Cookie，同步到青龙 |
| 收益统计 | `/earnings` | 查看金币收益、趋势图表 |
| 结算中心 | `/settlement-center` | 查看应缴、提交缴费凭证 |
| 我的钱包 | `/wallet` | 查看余额、账本流水、发起提现 |
| 推广中心 | `/referral` | 查看推广码、邀请统计、下级列表 |
| 个人账户 | `/account` | 修改个人信息、设置支付宝账号 |

### 管理员功能

| 页面 | 路由 | 说明 |
|------|------|------|
| 用户管理 | `/admin/users` | 查看/编辑用户、KSCK 管理 |
| 青龙实例 | `/admin/ql-instances` | 管理青龙面板实例 |
| 推广关系 | `/admin/referrals` | 查看推广关系图谱 |
| 缴费审核 | `/admin/settlement-payments` | 审核用户缴费申请 |
| 提现审核 | `/admin/withdraw-requests` | 审核用户提现申请 |
| 充值管理 | `/admin/recharge` | 查看充值订单、手动确认 |
| 支付宝配置 | `/admin/alipay-config` | 配置支付宝收款信息、分账比例 |

---

## 🔧 环境变量说明

| 变量 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `DATABASE_URL` | ✅ | - | MySQL 连接串 |
| `ADMIN_SECRET` | ❌ | `ADMIN_SECRET_KEY_2024` | 创建管理员 API 的密钥 |
| `ALLOW_MULTIPLE_ADMINS` | ❌ | `false` | 是否允许多个管理员 |
| `LOG_LEVEL` | ❌ | `INFO` | 日志级别 |
| `LOG_DIR` | ❌ | `logs` | 日志目录 |
| `LOG_MAX_BYTES` | ❌ | `10485760` | 单个日志文件最大字节数 |
| `LOG_BACKUP_COUNT` | ❌ | `5` | 日志文件备份数量 |

---

## 📁 项目结构

```
get_pretty_advantages/
├── app/
│   ├── auth.py              # JWT 认证
│   ├── database.py          # 数据库连接
│   ├── main.py              # FastAPI 应用入口
│   ├── models.py            # SQLAlchemy 数据模型
│   ├── routes/              # API 路由
│   │   ├── auth.py          # 认证相关
│   │   ├── users.py         # 用户管理
│   │   ├── ql_instances.py  # 青龙实例管理
│   │   ├── config_envs.py   # 环境变量配置 ⭐
│   │   ├── earnings.py      # 收益统计
│   │   ├── settlements.py   # 结算中心
│   │   ├── wallet.py        # 钱包系统
│   │   ├── withdrawals.py   # 提现管理
│   │   ├── referrals.py     # 推广关系
│   │   ├── recharge.py      # 充值分账
│   │   ├── stats.py         # 统计接口
│   │   └── admin.py         # 管理员功能
│   ├── schemas.py           # Pydantic 数据模型
│   └── services/            # 业务逻辑
│       ├── qinglong.py      # 青龙面板集成
│       └── settlement_unlock.py # 分成解锁
├── templates/               # Jinja2 模板
├── static/                  # 静态资源
├── data/                    # 数据目录（需挂载）
│   ├── software/            # APK 文件
│   └── describe/            # 说明文档、图片
├── logs/                    # 日志目录
├── docker-compose.yml       # Docker 编排配置
├── requirements.txt         # Python 依赖
└── README.md                # 项目说明
```

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

## 📄 许可证

[MIT License](LICENSE)

---

<div align="center">

**💡 提示**：本项目仅供学习交流使用，请遵守相关平台的使用条款。

</div>
