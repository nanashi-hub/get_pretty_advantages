// API基础URL
const API_BASE_URL = '/api';

// Token存储键名
const TOKEN_KEY = 'access_token';
const USER_KEY = 'current_user';

// ==================== Token 管理 ====================

function getToken() {
    return localStorage.getItem(TOKEN_KEY);
}

function saveToken(token) {
    localStorage.setItem(TOKEN_KEY, token);
}

function removeToken() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
}

function getCurrentUser() {
    const userStr = localStorage.getItem(USER_KEY);
    return userStr ? JSON.parse(userStr) : null;
}

function saveCurrentUser(user) {
    localStorage.setItem(USER_KEY, JSON.stringify(user));
}

// ==================== 错误消息 ====================

function showError(elementId, message) {
    const errorElement = document.getElementById(elementId);
    if (errorElement) {
        errorElement.textContent = message;
        errorElement.style.display = 'block';
    }
}

function hideError(elementId) {
    const errorElement = document.getElementById(elementId);
    if (errorElement) {
        errorElement.style.display = 'none';
    }
}

// ==================== Toast 通知 ====================

function showToast(message, type = 'info', duration = 3000) {
    const container = document.getElementById('toastContainer');
    if (!container) {
        // 如果没有容器，创建一个
        const newContainer = document.createElement('div');
        newContainer.id = 'toastContainer';
        newContainer.className = 'toast-container';
        document.body.appendChild(newContainer);
    }
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    
    const toastContainer = document.getElementById('toastContainer');
    toastContainer.appendChild(toast);
    
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(100%)';
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

// ==================== API 请求 ====================

async function apiRequest(url, options = {}) {
    const token = getToken();
    const headers = {
        'Content-Type': 'application/json',
        ...options.headers
    };
    
    if (token) {
        headers['Authorization'] = `Bearer ${token}`;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}${url}`, {
            ...options,
            headers
        });
        
        // 检查响应类型
        const contentType = response.headers.get('content-type');
        let data;
        
        if (contentType && contentType.includes('application/json')) {
            data = await response.json();
        } else {
            const text = await response.text();
            console.error('非JSON响应:', text);
            throw new Error(`服务器错误: ${response.status} ${response.statusText}`);
        }
        
        if (!response.ok) {
            // 401 未授权，跳转登录
            if (response.status === 401) {
                removeToken();
                window.location.href = '/login';
                return;
            }
            throw new Error(data.detail || data.message || `请求失败: ${response.status}`);
        }
        
        return data;
    } catch (error) {
        if (error instanceof TypeError) {
            throw new Error('网络连接失败，请检查网络或服务器状态');
        }
        if (error instanceof SyntaxError) {
            throw new Error('服务器返回了无效的响应格式');
        }
        throw error;
    }
}

// ==================== 侧边栏控制 ====================

function initSidebarScrollPersistence(sidebar) {
    const sidebarNav = sidebar.querySelector('.sidebar-nav');
    if (!sidebarNav) return;

    const storageKey = 'sidebarNavScrollTop';
    const saved = sessionStorage.getItem(storageKey);
    if (saved !== null) {
        const top = Number.parseInt(saved, 10);
        if (Number.isFinite(top) && top >= 0) {
            requestAnimationFrame(() => {
                sidebarNav.scrollTop = top;
            });
        }
    }

    let saveTimer = null;
    const persist = () => {
        sessionStorage.setItem(storageKey, String(sidebarNav.scrollTop));
    };

    sidebarNav.addEventListener('scroll', () => {
        if (saveTimer) window.clearTimeout(saveTimer);
        saveTimer = window.setTimeout(persist, 150);
    }, { passive: true });

    // 点击导航前先持久化滚动位置，避免快速跳转丢失
    sidebarNav.addEventListener('click', (e) => {
        const link = e.target && e.target.closest ? e.target.closest('a') : null;
        if (link) persist();
    }, true);

    window.addEventListener('beforeunload', persist);
}

function initSidebar() {
    const sidebar = document.getElementById('sidebar');
    if (!sidebar) return;

    const mobileMenuBtn = document.getElementById('mobileMenuBtn');
    const sidebarToggle = document.getElementById('sidebarToggle');
    
    if (mobileMenuBtn) {
        mobileMenuBtn.addEventListener('click', () => {
            sidebar.classList.toggle('open');
        });
    }
    
    if (sidebarToggle) {
        sidebarToggle.addEventListener('click', () => {
            sidebar.classList.toggle('collapsed');
        });
    }
    
    // 点击侧边栏外部关闭（移动端）
    document.addEventListener('click', (e) => {
        if (window.innerWidth <= 768) {
            if (!sidebar.contains(e.target) && (!mobileMenuBtn || !mobileMenuBtn.contains(e.target))) {
                sidebar.classList.remove('open');
            }
        }
    });

    // 记住侧边栏滚动位置，页面跳转后自动恢复
    initSidebarScrollPersistence(sidebar);
}

// ==================== 用户信息加载 ====================

async function loadCurrentUser() {
    try {
        const user = await apiRequest('/me');
        saveCurrentUser(user);
        updateUserDisplay(user);
        updateAdminVisibility(user);

        const serviceMode = await loadServiceMode();
        updateCommercialVisibility(serviceMode);

        return user;
    } catch (error) {
        console.error('获取用户信息失败:', error);
        return null;
    }
}

function updateUserDisplay(user) {
    // 侧边栏用户信息
    const sidebarUserName = document.getElementById('sidebarUserName');
    const sidebarUserRole = document.getElementById('sidebarUserRole');
    const userAvatar = document.getElementById('userAvatar');
    
    if (sidebarUserName) {
        sidebarUserName.textContent = user.nickname || user.username;
    }
    if (sidebarUserRole) {
        const roleMap = { 'admin': '管理员', 'agent': '代理', 'normal': '普通用户' };
        sidebarUserRole.textContent = roleMap[user.role] || user.role;
    }
    if (userAvatar) {
        userAvatar.textContent = (user.nickname || user.username).charAt(0).toUpperCase();
    }
    
    // 头部用户信息
    const headerUserName = document.getElementById('headerUserName');
    const headerUserRole = document.getElementById('headerUserRole');
    
    if (headerUserName) {
        headerUserName.textContent = user.nickname || user.username;
    }
    if (headerUserRole) {
        const roleMap = { 'admin': '管理员', 'agent': '代理', 'normal': '普通用户' };
        headerUserRole.textContent = roleMap[user.role] || user.role;
    }
    
    // 旧仪表板兼容
    const currentUserElement = document.getElementById('currentUser');
    if (currentUserElement) {
        const displayName = user.nickname || user.username;
        currentUserElement.textContent = `${displayName} (${user.username})`;
    }
    
    const roleElement = document.getElementById('userRole');
    if (roleElement) {
        const roleMap = { 'admin': '管理员', 'agent': '代理', 'normal': '普通用户' };
        roleElement.textContent = roleMap[user.role] || user.role;
        roleElement.className = `user-role role-${user.role}`;
    }
}

function updateAdminVisibility(user) {
    // 根据用户角色显示/隐藏管理员菜单
    const adminElements = document.querySelectorAll('.admin-only');
    const isAdmin = user.role === 'admin';
    
    adminElements.forEach(el => {
        el.style.display = isAdmin ? '' : 'none';
    });
}

function updateCommercialVisibility(serviceMode) {
    const isCommercial = serviceMode === 'commercial';
    const commercialElements = document.querySelectorAll('.commercial-only');

    commercialElements.forEach(el => {
        el.style.display = isCommercial ? '' : 'none';
    });

    // 公益版：避免通过直接 URL 访问用户页
    if (!isCommercial) {
        const restrictedPaths = ['/settlement-center', '/wallet'];
        if (restrictedPaths.includes(window.location.pathname)) {
            window.location.href = '/dashboard';
        }
    }
}

async function loadServiceMode() {
    try {
        const data = await apiRequest('/service-mode');
        return data && data.service_mode ? data.service_mode : 'commercial';
    } catch (e) {
        return 'commercial';
    }
}

// ==================== 登录处理 ====================

if (document.getElementById('loginForm')) {
    document.getElementById('loginForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        hideError('errorMessage');
        
        const formData = {
            username_or_email: document.getElementById('username_or_email').value,
            password: document.getElementById('password').value
        };
        
        try {
            const result = await apiRequest('/login', {
                method: 'POST',
                body: JSON.stringify(formData)
            });
            
            saveToken(result.access_token);
            saveCurrentUser(result.user);
            window.location.href = '/dashboard';
        } catch (error) {
            showError('errorMessage', error.message || '登录失败，请检查用户名和密码');
        }
    });
}

// ==================== 注册处理 ====================

if (document.getElementById('registerForm')) {
    document.getElementById('registerForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        hideError('errorMessage');
        
        const password = document.getElementById('password').value;
        const confirmPassword = document.getElementById('confirmPassword').value;
        
        if (password !== confirmPassword) {
            showError('errorMessage', '两次输入的密码不一致');
            return;
        }
        
        const formData = {
            username: document.getElementById('username').value,
            password: password,
            nickname: document.getElementById('nickname').value || null,
            phone: document.getElementById('phone').value || null,
            wechat_id: document.getElementById('wechat_id').value || null,
            invite_code: document.getElementById('invite_code').value || null
        };
        
        // 移除空值
        Object.keys(formData).forEach(key => {
            if (formData[key] === null || formData[key] === '') {
                delete formData[key];
            }
        });
        
        try {
            const result = await apiRequest('/register', {
                method: 'POST',
                body: JSON.stringify(formData)
            });
            
            saveToken(result.access_token);
            saveCurrentUser(result.user);
            window.location.href = '/dashboard';
        } catch (error) {
            showError('errorMessage', error.message || '注册失败，请检查输入信息');
        }
    });
}

// ==================== 登出处理 ====================

function setupLogout() {
    const logoutBtn = document.getElementById('logoutBtn');
    if (logoutBtn) {
        logoutBtn.addEventListener('click', () => {
            removeToken();
            window.location.href = '/login';
        });
    }
}

// ==================== 旧仪表板兼容 - 用户列表加载 ====================

async function loadUsers() {
    const loadingElement = document.getElementById('loadingMessage');
    const errorElement = document.getElementById('errorMessage');
    const tableElement = document.getElementById('usersTable');
    const tableBodyElement = document.getElementById('usersTableBody');
    
    if (!tableBodyElement) return;
    
    try {
        const users = await apiRequest('/users');
        
        if (loadingElement) loadingElement.style.display = 'none';
        if (errorElement) errorElement.style.display = 'none';
        if (tableElement) tableElement.style.display = 'table';
        
        tableBodyElement.innerHTML = '';
        
        const totalUsersElement = document.getElementById('totalUsers');
        if (totalUsersElement) {
            totalUsersElement.textContent = users.length;
        }
        
        if (users.length === 0) {
            tableBodyElement.innerHTML = '<tr><td colspan="8" style="text-align: center;">暂无用户</td></tr>';
            return;
        }
        
        const roleMap = { 'admin': '管理员', 'agent': '代理', 'normal': '普通用户' };
        const statusMap = {
            1: '<span class="status-badge status-active">正常</span>',
            0: '<span class="status-badge status-inactive">禁用</span>'
        };
        
        users.forEach(user => {
            const row = document.createElement('tr');
            const createdDate = new Date(user.created_at).toLocaleString('zh-CN');
            
            row.innerHTML = `
                <td>${user.id}</td>
                <td>${user.username}</td>
                <td>${user.nickname || '-'}</td>
                <td>${user.phone || '-'}</td>
                <td>${user.wechat_id || '-'}</td>
                <td><span class="role-badge role-${user.role}">${roleMap[user.role] || user.role}</span></td>
                <td>${statusMap[user.status] || user.status}</td>
                <td>${createdDate}</td>
            `;
            
            tableBodyElement.appendChild(row);
        });
    } catch (error) {
        if (loadingElement) loadingElement.style.display = 'none';
        if (tableElement) tableElement.style.display = 'none';
        if (errorElement) {
            showError('errorMessage', error.message || '加载用户列表失败');
        }
        
        if (error.message.includes('401') || error.message.includes('无法验证')) {
            removeToken();
            window.location.href = '/login';
        }
    }
}

// ==================== 页面初始化 ====================

document.addEventListener('DOMContentLoaded', () => {
    // 初始化侧边栏
    initSidebar();
    
    // 设置登出按钮
    setupLogout();
    
    // 判断是否需要登录
    const isAuthPage = document.getElementById('loginForm') || document.getElementById('registerForm');
    const token = getToken();
    
    if (isAuthPage) {
        // 认证页面：如果已登录则跳转
        if (token) {
            apiRequest('/me').then(() => {
                window.location.href = '/dashboard';
            }).catch(() => {
                removeToken();
            });
        }
    } else {
        // 其他页面：需要登录
        if (!token) {
            window.location.href = '/login';
            return;
        }
        
        // 加载用户信息
        loadCurrentUser();
        
        // 旧仪表板兼容（排除用户管理页面）
        const isUserAdminPage = window.location.pathname === '/admin/users';
        if (!isUserAdminPage && document.getElementById('usersTable') && document.getElementById('usersTableBody')) {
            loadUsers();
        }
    }
});
