# app/services/qinglong.py
import time
from typing import Any, Dict, List, Optional, Union

import requests

from app.models import QLInstance


class QingLongClient:
    """青龙面板 API 客户端"""
    
    def __init__(self, instance: QLInstance):
        self.base_url = instance.base_url.rstrip("/")
        self.client_id = instance.client_id
        self.client_secret = instance.client_secret
        self._token: Optional[str] = None
        self._expire_at: float = 0.0

    def _get_token(self) -> str:
        """获取或刷新 token"""
        now = time.time()
        if self._token and now < self._expire_at - 60:
            return self._token

        url = f"{self.base_url}/open/auth/token"
        r = requests.get(
            url,
            params={"client_id": self.client_id, "client_secret": self.client_secret},
            timeout=10,
        )
        r.raise_for_status()

        data = r.json()
        if data.get("code") != 200:
            raise RuntimeError(f"获取青龙 token 失败: {data}")

        token = data["data"]["token"]
        expiration = data["data"].get("expiration") or 3600

        self._token = token
        self._expire_at = now + float(expiration)
        return token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json;charset=UTF-8",
        }

    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """通用请求方法"""
        url = f"{self.base_url}{endpoint}"
        kwargs.setdefault("headers", self._headers())
        kwargs.setdefault("timeout", 15)

        r = requests.request(method, url, **kwargs)

        # 尝试解析响应
        try:
            data = r.json()
        except Exception:
            # 如果不是 JSON 响应，抛出详细错误
            error_msg = f"青龙请求失败: {method} {url} -> HTTP {r.status_code}\n响应内容: {r.text[:500]}"
            raise RuntimeError(error_msg)

        # 检查 HTTP 状态码
        if r.status_code >= 400:
            error_msg = f"青龙请求失败: {method} {url} -> HTTP {r.status_code}\n响应: {data}"
            raise RuntimeError(error_msg)

        # 检查业务状态码
        if data.get("code") != 200:
            raise RuntimeError(f"青龙 API 错误: {data.get('message', data)}")
        return data

    # ==================== 连通性测试 ====================
    
    def ping(self) -> Dict[str, Any]:
        """连通性测试：能否成功拿到 token"""
        token = self._get_token()
        return {"ok": True, "token_prefix": token[:12]}

    # ==================== 环境变量管理 ====================

    def list_envs(self, search_value: str = "") -> List[Dict[str, Any]]:
        """查询环境变量列表"""
        params = {"searchValue": search_value} if search_value else {}
        data = self._request("GET", "/open/envs", params=params)
        return data.get("data", [])

    def get_env_by_id(self, env_id: Union[str, int]) -> Optional[Dict[str, Any]]:
        """根据ID获取单个环境变量"""
        try:
            data = self._request("GET", f"/open/envs/{env_id}")
            return data.get("data")
        except Exception:
            return None

    def create_env(self, name: str, value: str, remarks: str = "") -> Dict[str, Any]:
        """创建环境变量"""
        payload = [{"name": name, "value": value, "remarks": remarks}]
        data = self._request("POST", "/open/envs", json=payload)
        # 青龙返回的是列表
        result = data.get("data", [])
        if isinstance(result, list) and result:
            return result[0]
        return result

    def create_envs_batch(self, envs: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """批量创建环境变量
        
        Args:
            envs: [{"name": "xxx", "value": "xxx", "remarks": "xxx"}, ...]
        """
        if not envs:
            return []
        data = self._request("POST", "/open/envs", json=envs)
        return data.get("data", [])

    def update_env(self, env_id: Union[str, int], name: str, value: str, remarks: str = "") -> Dict[str, Any]:
        """更新环境变量"""
        payload = {"id": env_id, "name": name, "value": value, "remarks": remarks}
        data = self._request("PUT", "/open/envs", json=payload)
        return data.get("data", {})

    def delete_envs(self, env_ids: List[Union[str, int]]) -> bool:
        """删除环境变量（批量）"""
        if not env_ids:
            return True
        self._request("DELETE", "/open/envs", json=env_ids)
        return True

    def delete_env(self, env_id: Union[str, int]) -> bool:
        """删除单个环境变量"""
        return self.delete_envs([env_id])

    def enable_envs(self, env_ids: List[Union[str, int]]) -> bool:
        """启用环境变量（批量）"""
        if not env_ids:
            return True
        self._request("PUT", "/open/envs/enable", json=env_ids)
        return True

    def enable_env(self, env_id: Union[str, int]) -> bool:
        """启用单个环境变量"""
        return self.enable_envs([env_id])

    def disable_envs(self, env_ids: List[Union[str, int]]) -> bool:
        """禁用环境变量（批量）"""
        if not env_ids:
            return True
        self._request("PUT", "/open/envs/disable", json=env_ids)
        return True

    def disable_env(self, env_id: Union[str, int]) -> bool:
        """禁用单个环境变量"""
        return self.disable_envs([env_id])

    # ==================== 便捷方法 ====================

    def find_env_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """根据变量名查找环境变量（精确匹配）"""
        envs = self.list_envs(search_value=name)
        for env in envs:
            if env.get("name") == name:
                return env
        return None

    def upsert_env(self, name: str, value: str, remarks: str = "") -> Dict[str, Any]:
        """创建或更新环境变量（根据名称判断）
        
        如果同名变量存在则更新，否则创建新的
        """
        existing = self.find_env_by_name(name)
        if existing:
            env_id = existing.get("id") or existing.get("_id")
            return self.update_env(env_id, name, value, remarks)
        else:
            return self.create_env(name, value, remarks)

    def sync_env(self, name: str, value: str, remarks: str = "", enabled: bool = True) -> Dict[str, Any]:
        """同步环境变量（创建/更新 + 启用/禁用）"""
        result = self.upsert_env(name, value, remarks)
        env_id = result.get("id") or result.get("_id")
        
        if env_id:
            if enabled:
                self.enable_env(env_id)
            else:
                self.disable_env(env_id)
        
        return result
