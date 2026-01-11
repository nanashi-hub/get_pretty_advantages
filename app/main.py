from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, RedirectResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy import text
from starlette.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from app.database import engine, init_db
from app.logging_config import setup_logging_from_env, get_logger
from app.routes import auth, users, admin, account
from app.routes import (
    ql_instances,
    script_configs,
    earnings,
    settlements,
    wallet,
    withdrawals,
    referrals,
    stats,
    config_envs,
    recharge,
    system_settings,
)
import traceback

# 获取项目根目录（app目录的父目录）
BASE_DIR = Path(__file__).resolve().parent.parent

# 静态文件和模板目录的绝对路径
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"
DATA_DIR = BASE_DIR / "data"


def get_service_mode() -> str:
    """获取当前服务模式（commercial/public），默认 commercial。"""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT setting_value
                    FROM system_settings
                    WHERE setting_key = 'service_mode'
                    LIMIT 1
                    """
                )
            ).fetchone()
            if row and row[0] == "public":
                return "public"
    except Exception:
        return "commercial"
    return "commercial"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 初始化日志配置
    setup_logging_from_env()
    logger = get_logger(__name__)
    logger.info("=" * 50)
    logger.info("应用启动中...")
    logger.info("=" * 50)

    # 启动时执行
    init_db()
    logger.info("数据库初始化完成")
    print("数据库初始化完成")

    # 启动定时调度器（用于检查支付状态）
    from app.services.scheduler import start_scheduler
    start_scheduler()
    logger.info("定时调度器已启动")
    print("定时调度器已启动")

    yield

    # 关闭时执行
    from app.services.scheduler import stop_scheduler
    stop_scheduler()
    logger.info("定时调度器已停止")
    print("定时调度器已停止")


# 创建FastAPI应用
app = FastAPI(
    title="快手账号管理平台",
    version="1.0.0",
    lifespan=lifespan
)

# 挂载静态文件（使用绝对路径）
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# 挂载 data 目录（用于文件下载）
if DATA_DIR.exists():
    app.mount("/data", StaticFiles(directory=str(DATA_DIR)), name="data")

# 配置模板（使用绝对路径）
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# 全局异常处理器
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """处理HTTP异常，确保返回JSON格式"""
    # 如果是API请求，返回JSON
    if request.url.path.startswith("/api"):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail}
        )
    # 对于非API请求，重新抛出异常让FastAPI默认处理
    raise exc


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """处理请求验证错误"""
    if request.url.path.startswith("/api"):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": exc.errors()}
        )
    # 对于非API请求，重新抛出异常
    raise exc


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """处理所有未捕获的异常"""
    logger = get_logger(__name__)
    # 记录错误详情
    logger.error(f"未处理的异常: {exc}", exc_info=True)
    print(f"未处理的异常: {exc}")
    traceback.print_exc()
    
    # 如果是API请求，返回JSON格式的错误
    if request.url.path.startswith("/api"):
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": f"服务器内部错误: {str(exc)}"
            }
        )
    # 对于非API请求，返回通用错误页面
    from fastapi import HTTPException
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="服务器内部错误，请稍后重试"
    )


# ==================== 注册API路由 ====================
app.include_router(auth.router)
app.include_router(users.router)
app.include_router(admin.router)
app.include_router(account.router)
app.include_router(ql_instances.router)
app.include_router(script_configs.router)
app.include_router(earnings.router)
app.include_router(settlements.router)
app.include_router(wallet.router)
app.include_router(withdrawals.router)
app.include_router(referrals.router)
app.include_router(stats.router)
app.include_router(config_envs.router)
app.include_router(system_settings.router)

# 充值分账模块路由
from app.routes import alipay_config
app.include_router(recharge.router)
app.include_router(alipay_config.router)


# ==================== 页面路由 ====================

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """根路径重定向到登录页"""
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """登录页面"""
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    """注册页面"""
    return templates.TemplateResponse("register.html", {"request": request})


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    """仪表板页面"""
    return templates.TemplateResponse("dashboard_new.html", {"request": request, "active_page": "dashboard"})


@app.get("/ks-accounts", response_class=HTMLResponse)
async def ks_accounts_page(request: Request):
    """快手账号管理页面"""
    return templates.TemplateResponse("ks_accounts.html", {"request": request, "active_page": "ks_accounts"})


@app.get("/config-envs", response_class=HTMLResponse)
async def config_envs_page(request: Request):
    """配置环境管理页面"""
    return templates.TemplateResponse(
        "config_envs.html",
        {"request": request, "active_page": "config_envs"},
    )


@app.get("/earnings", response_class=HTMLResponse)
async def earnings_page(request: Request):
    """收益统计页面"""
    return templates.TemplateResponse("earnings.html", {"request": request, "active_page": "earnings"})


@app.get("/settlement-center", response_class=HTMLResponse)
async def settlement_center_page(request: Request):
    """结算中心页面"""
    if get_service_mode() != "commercial":
        return RedirectResponse(url="/dashboard", status_code=302)
    return templates.TemplateResponse(
        "settlement_center.html",
        {"request": request, "active_page": "settlement_center"},
    )


@app.get("/wallet", response_class=HTMLResponse)
async def wallet_page(request: Request):
    """我的钱包页面"""
    if get_service_mode() != "commercial":
        return RedirectResponse(url="/dashboard", status_code=302)
    return templates.TemplateResponse("wallet.html", {"request": request, "active_page": "wallet"})


@app.get("/referral", response_class=HTMLResponse)
async def referral_page(request: Request):
    """推广中心页面"""
    return templates.TemplateResponse("referral.html", {"request": request, "active_page": "referral"})

@app.get("/account", response_class=HTMLResponse)
async def account_page(request: Request):
    """个人账户页面"""
    return templates.TemplateResponse("account.html", {"request": request, "active_page": "account"})


# 避免 favicon 404 打出 500 日志
@app.get("/favicon.ico")
async def favicon():
    from fastapi import Response
    return Response(status_code=204)


# ==================== 文档和下载 API ====================

@app.get("/api/guide/content")
async def get_guide_content(request: Request):
    """获取新手搭建说明文档内容（Markdown 格式）"""
    md_path = DATA_DIR / "describe" / "新手搭建说明.md"

    if not md_path.exists():
        return JSONResponse(
            status_code=404,
            content={"detail": "说明文档不存在"}
        )

    try:
        import markdown
        import re

        with open(md_path, "r", encoding="utf-8") as f:
            md_content = f.read()

        # 获取服务器基础URL（用于图片路径）
        scheme = request.url.scheme
        server_host = request.headers.get("host", "localhost:1212")
        base_url = f"{scheme}://{server_host}"

        # 将 Markdown 中的相对图片路径替换为完整URL
        # 匹配 /data/ 开头的图片路径
        def replace_image_path(match):
            img_path = match.group(1)
            # 如果是 /data/ 开头的路径，添加完整URL
            if img_path.startswith("/data/"):
                return f"]({base_url}{img_path})"
            return f"]({img_path})"

        # 替换 markdown 图片语法 ](/data/xxx)
        md_content = re.sub(r"\]\((/data/[^\)]+)\)", replace_image_path, md_content)

        # 将 Markdown 转换为 HTML，启用常用扩展
        html_content = markdown.markdown(
            md_content,
            extensions=['tables', 'fenced_code', 'nl2br', 'sane_lists']
        )

        # 添加图片自适应样式和点击全屏功能
        styled_content = f"""
        <style>
            .markdown-body img {{
                max-width: 100%;
                height: auto;
                display: block;
                margin: 10px 0;
                border-radius: 4px;
                cursor: pointer;
                transition: transform 0.2s;
            }}
            .markdown-body img:hover {{
                opacity: 0.85;
            }}
            .markdown-body table {{
                border-collapse: collapse;
                width: 100%;
                margin: 10px 0;
            }}
            .markdown-body table th,
            .markdown-body table td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            .markdown-body table th {{
                background-color: #f2f2f2;
            }}
            /* 全屏图片遮罩层 */
            .image-overlay {{
                display: none;
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background-color: rgba(0, 0, 0, 0.9);
                z-index: 9999;
                justify-content: center;
                align-items: center;
                cursor: zoom-out;
            }}
            .image-overlay.active {{
                display: flex;
            }}
            .image-overlay img {{
                max-width: 95%;
                max-height: 95%;
                object-fit: contain;
                cursor: zoom-out;
            }}
            .image-overlay .close-hint {{
                position: absolute;
                top: 20px;
                color: white;
                font-size: 14px;
                background: rgba(0,0,0,0.5);
                padding: 8px 16px;
                border-radius: 4px;
            }}
        </style>
        <div class="markdown-body">{html_content}</div>
        <div class="image-overlay" id="imageOverlay">
            <span class="close-hint">点击图片或按 ESC 关闭</span>
            <img id="overlayImage" src="" alt="全屏图片">
        </div>
        <script>
        (function() {{
            const overlay = document.getElementById('imageOverlay');
            const overlayImg = document.getElementById('overlayImage');

            // 为所有图片添加点击事件
            document.querySelectorAll('.markdown-body img').forEach(img => {{
                img.addEventListener('click', function(e) {{
                    e.preventDefault();
                    overlayImg.src = this.src;
                    overlay.classList.add('active');
                    document.body.style.overflow = 'hidden';
                }});
            }});

            // 点击遮罩层关闭
            overlay.addEventListener('click', function() {{
                closeOverlay();
            }});

            // 按 ESC 关闭
            document.addEventListener('keydown', function(e) {{
                if (e.key === 'Escape') {{
                    closeOverlay();
                }}
            }});

            function closeOverlay() {{
                overlay.classList.remove('active');
                document.body.style.overflow = '';
            }}
        }})();
        </script>
        """
        return {"content": styled_content, "base_url": base_url}
    except ImportError:
        return JSONResponse(
            status_code=500,
            content={"detail": "服务器缺少 markdown 库，请安装: pip install markdown"}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"读取文档失败: {str(e)}"}
        )


# ==================== 管理员页面路由 ====================

@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users_page(request: Request):
    """用户管理页面（管理员）"""
    return templates.TemplateResponse("admin_users.html", {"request": request, "active_page": "users"})


@app.get("/admin/ql-instances", response_class=HTMLResponse)
async def admin_ql_instances_page(request: Request):
    """青龙实例管理页面（管理员）"""
    return templates.TemplateResponse("admin_ql_instances.html", {"request": request, "active_page": "ql_instances"})


@app.get("/admin/ip-pool", response_class=HTMLResponse)
async def admin_ip_pool_page(request: Request):
    """系统 IP 池管理页面（管理员）"""
    return templates.TemplateResponse("admin_ip_pool.html", {"request": request, "active_page": "ip_pool"})


@app.get("/admin/referrals", response_class=HTMLResponse)
async def admin_referrals_page(request: Request):
    """推广关系管理页面（管理员）"""
    return templates.TemplateResponse("admin_referrals.html", {"request": request, "active_page": "referrals"})


@app.get("/recharge", response_class=HTMLResponse)
async def recharge_page(request: Request):
    """充值中心页面"""
    return templates.TemplateResponse("recharge.html", {"request": request, "active_page": "recharge"})


@app.get("/admin/recharge", response_class=HTMLResponse)
async def admin_recharge_page(request: Request):
    """充值管理页面（管理员）"""
    return templates.TemplateResponse("admin_recharge.html", {"request": request, "active_page": "recharge"})


@app.get("/admin/alipay-config", response_class=HTMLResponse)
async def admin_alipay_config_page(request: Request):
    """支付宝配置管理页面（管理员）"""
    return templates.TemplateResponse("admin_alipay_config.html", {"request": request, "active_page": "alipay_config"})


@app.get("/admin/settlement-payments", response_class=HTMLResponse)
async def admin_settlement_payments_page(request: Request):
    """缴费审核页面（管理员）"""
    return templates.TemplateResponse(
        "admin_settlement_payments.html",
        {"request": request, "active_page": "settlement_payments"},
    )


@app.get("/admin/withdraw-requests", response_class=HTMLResponse)
async def admin_withdraw_requests_page(request: Request):
    """提现审核页面（管理员）"""
    return templates.TemplateResponse(
        "admin_withdraw_requests.html",
        {"request": request, "active_page": "withdraw_requests"},
    )


@app.get("/admin/ban-reports", response_class=HTMLResponse)
async def admin_ban_reports_page(request: Request):
    """封号提报审核页面（管理员）"""
    return templates.TemplateResponse(
        "admin_ban_reports.html",
        {"request": request, "active_page": "ban_reports"},
    )


@app.get("/admin/service-mode", response_class=HTMLResponse)
async def admin_service_mode_page(request: Request):
    """服务模式切换（管理员）"""
    return templates.TemplateResponse(
        "admin_service_mode.html",
        {"request": request, "active_page": "service_mode"},
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1212)
