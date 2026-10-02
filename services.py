"""业务侧缓存访问封装：现有读取路径全部收敛到这里。

加载器职责：
1. 回源（通过可注入的 :class:`MockUpstream`）；
2. 把上游“明确缺失”转成 ``Loaded(missing=True)``（走短 TTL 负缓存）；
3. 故障 / 超时 / 格式错误原样向上抛，绝不伪装成缺失；
4. 声明派生结果依赖的实体 id，供精准失效使用。
"""

from __future__ import annotations

from cachelayer.cache import CacheLayer
from cachelayer.errors import UpstreamNotFoundError
from cachelayer.models import CacheResult, Loaded
from cachelayer.upstream import MockUpstream


class UserService:
    def __init__(self, cache: CacheLayer, upstream: MockUpstream) -> None:
        self._cache = cache
        self._upstream = upstream

    def get_user(self, user_id: str) -> CacheResult:
        return self._cache.get(f"user:{user_id}", lambda: self._load_user(user_id))

    def _load_user(self, user_id: str) -> Loaded:
        try:
            data = self._upstream.get_user(user_id)
        except UpstreamNotFoundError:
            # 明确不存在：允许短 TTL 负缓存
            return Loaded(value=None, dependencies=(f"user:{user_id}",), missing=True)
        # unavailable / timeout / format 错误不捕获、不缓存
        return Loaded(value=data, dependencies=(f"user:{user_id}",))

    def invalidate(self, user_id: str) -> bool:
        return self._cache.invalidate_dependency(f"user:{user_id}") > 0


class FeedService:
    def __init__(self, cache: CacheLayer, upstream: MockUpstream) -> None:
        self._cache = cache
        self._upstream = upstream

    def get_feed(self, feed_id: str) -> CacheResult:
        return self._cache.get(f"feed:{feed_id}", lambda: self._load_feed(feed_id))

    def _load_feed(self, feed_id: str) -> Loaded:
        try:
            data = self._upstream.get_feed(feed_id)
        except UpstreamNotFoundError:
            # feed 本身不存在：依赖 feed 实体自身的“存在性”
            return Loaded(value=None, dependencies=(f"feed:{feed_id}",), missing=True)
        deps = [f"feed:{feed_id}"]
        for u in data.get("users", []):  # 组合结果依赖每个成员
            deps.append(f"user:{u['id']}")
        return Loaded(value=data, dependencies=deps)

    def invalidate_feed(self, feed_id: str) -> bool:
        return self._cache.invalidate_dependency(f"feed:{feed_id}") > 0
