"""Two tiny apps for bench/web_demo.py, limited to 100 requests per minute per client address.

flask_app: Flask + Flask-Limiter (run under gunicorn). fastapi_app: FastAPI + slowapi (run under
uvicorn). The storage comes from $STORAGE: memory:// (each library's default) or velocity://NAME.
Every response carries X-Worker, the pid of the worker process that served it.
"""
import os

import velocity_limits  # noqa: F401  registers the velocity:// storage scheme

STORAGE = os.environ.get("STORAGE", "memory://")
LIMIT = "100/minute"

if os.environ.get("APP") == "flask":
    from flask import Flask, make_response
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address

    flask_app = Flask(__name__)
    flask_limiter = Limiter(get_remote_address, app=flask_app, storage_uri=STORAGE)

    @flask_app.route("/")
    @flask_limiter.limit(LIMIT)
    def flask_index():
        resp = make_response("ok")
        resp.headers["X-Worker"] = str(os.getpid())
        return resp

if os.environ.get("APP") == "fastapi":
    from fastapi import FastAPI, Request
    from fastapi.responses import PlainTextResponse
    from slowapi import Limiter as SlowLimiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
    from slowapi.util import get_remote_address as slow_remote_address

    slow_limiter = SlowLimiter(key_func=slow_remote_address, storage_uri=STORAGE)
    fastapi_app = FastAPI()
    fastapi_app.state.limiter = slow_limiter
    fastapi_app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    @fastapi_app.get("/")
    @slow_limiter.limit(LIMIT)
    async def fastapi_index(request: Request):
        return PlainTextResponse("ok", headers={"X-Worker": str(os.getpid())})
