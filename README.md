# 快手脚本平台（get_pretty_advantages）

基于 FastAPI + MySQL 的快手账号管理平台，提供账号环境配置、收益统计、结算/缴费、分成钱包、提现审核等能力。

## 更新日志

详见 `CHANGELOG.md`。

## 核心功能

- 用户体系：注册/登录/JWT、首个用户自动成为管理员
- 推广关系：邀请码绑定 +1/+2，上下级链路管理
- 账号环境：配置脚本环境（含备注/代理 IP 等）
- 收益统计：按周期查询与展示（以日收益汇总为基础）
- 结算中心（阶段 1）：结算期、关系快照、收益汇总、应缴、缴费记录、管理员审核
- 分成钱包（阶段 2）：分成明细、下级缴清后资金化入账（locked）、钱包账本流水
- 解锁/提现（阶段 3）：上级缴清后解锁（locked -> available），用户提现申请/取消、管理员审核/打款/驳回（支持回滚）

## 快速开始（Docker，推荐）

1) 准备 MySQL（建议单独容器或宿主机），创建库（示例）：

```sql
CREATE DATABASE get_pretty_advantages DEFAULT CHARACTER SET utf8mb4;
```

2) 配置 `DATABASE_URL`（推荐使用 `.env` 或 CI/容器环境变量注入），示例：

```bash
DATABASE_URL=mysql+pymysql://root:password@127.0.0.1:3306/get_pretty_advantages?charset=utf8mb4
ADMIN_SECRET=ADMIN_SECRET_KEY_2024
```

3) 启动：

```bash
docker compose up -d --build
```

4) 访问：

- Web：`http://localhost:1212`

## 本地开发（非 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 1212
```

## 关键环境变量

- `DATABASE_URL`：MySQL 连接串（必配，建议不要把真实账号密码提交到 GitHub）
- `ADMIN_SECRET`：创建管理员账号 API 的密钥（默认 `ADMIN_SECRET_KEY_2024`）
- `ALLOW_MULTIPLE_ADMINS`：是否允许多个管理员（默认 `false`）
- 日志：
  - `LOG_LEVEL`、`LOG_DIR`、`LOG_MAX_BYTES`、`LOG_BACKUP_COUNT`

## 管理员创建

- 方式 1：首个注册用户自动成为管理员（推荐）
- 方式 2：API 创建管理员（需要 `ADMIN_SECRET`）：

```bash
curl -X POST "http://localhost:1212/api/admin/create-admin?admin_secret=ADMIN_SECRET_KEY_2024" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"admin\",\"password\":\"admin123\",\"nickname\":\"管理员\"}"
```

## 常用页面入口

- 仪表板：`/dashboard`
- 配置环境：`/config-envs`
- 收益统计：`/earnings`
- 结算中心：`/settlement-center`
- 我的钱包：`/wallet`
- 管理端：`/admin/settlement-payments`、`/admin/withdraw-requests`

## 内置资料（/data）

应用会挂载 `/data` 静态目录，仪表板「新手入门指南」使用以下资源：

- APK 下载：`/data/software/快手极速版提ck.apk`
- 新手说明：`data/describe/新手搭建说明.md`（前端通过 `/api/guide/content` 展示）
- 进群二维码：`/data/describe/images/进群.jpg`

  <img src="data/describe/images/进群.jpg" alt="进群二维码" width="350" />
