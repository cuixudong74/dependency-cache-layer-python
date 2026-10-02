"""进程入口：装配模拟上游 + 缓存层 + HTTP 服务，关闭时优雅停止刷新。"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from api import create_app

app: FastAPI = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
