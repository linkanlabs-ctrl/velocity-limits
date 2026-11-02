"""The documented multi-worker bug, end to end: real servers, real libraries, 1,000 requests.

For each of Flask-Limiter under gunicorn and slowapi under uvicorn, with 4 worker processes and a
limit of 100 requests per minute, start the server with memory:// (the libraries' default) and
then with velocity://, send REQUESTS requests from 16 threads (a new connection each, so the
requests spread over the workers), and count how many were let through (200) or refused (429).
A correct limiter lets exactly 100 through.

Linux only (gunicorn). Run:
    pip install "velocity-limits[limits]" flask flask-limiter gunicorn fastapi slowapi uvicorn
    python3 benchmarks/web_demo.py
"""
import collections
import http.client
import os
import platform
import socket
import subprocess
import threading
import time
from importlib.metadata import version

import velocity_limits

HERE = os.path.dirname(os.path.abspath(__file__))

WORKERS, REQUESTS, THREADS, PORT = 4, 1000, 16, 8701
SERVERS = {
    "Flask-Limiter on gunicorn": ("flask", ["gunicorn", "-w", str(WORKERS), "-b", f"127.0.0.1:{PORT}",
                                            "--log-level", "warning", "web_demo_apps:flask_app"]),
    "slowapi on uvicorn": ("fastapi", ["uvicorn", "--workers", str(WORKERS), "--host", "127.0.0.1",
                                       "--port", str(PORT), "--log-level", "warning", "web_demo_apps:fastapi_app"]),
}


def wait_port(proc):
    for _ in range(300):
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with {proc.returncode}")
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def fire():
    statuses, workers, lock, todo = collections.Counter(), collections.Counter(), threading.Lock(), [REQUESTS]

    def run():
        while True:
            with lock:
                if todo[0] == 0:
                    return
                todo[0] -= 1
            conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=10)
            conn.request("GET", "/", headers={"Connection": "close"})
            r = conn.getresponse()
            r.read()
            conn.close()
            with lock:
                statuses[r.status] += 1
                if r.status == 200:
                    workers[r.getheader("X-Worker")] += 1

    threads = [threading.Thread(target=run) for _ in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return statuses, workers


def main():
    print(f"# {platform.platform()}, Python {platform.python_version()}; flask-limiter {version('flask-limiter')}, "
          f"gunicorn {version('gunicorn')}, slowapi {version('slowapi')}, uvicorn {version('uvicorn')}, "
          f"limits {version('limits')}, velocity_limits {velocity_limits.__version__}")
    print(f"# {WORKERS} workers, limit 100/minute per client, {REQUESTS} requests from {THREADS} threads\n")
    for label, (app, cmd) in SERVERS.items():
        for storage in ("memory://", f"velocity://demo-{app}-{os.getpid()}"):
            name = storage.split("//")[1]
            if storage.startswith("velocity"):
                velocity_limits.remove(name)
            env = dict(os.environ, APP=app, STORAGE=storage)
            proc = subprocess.Popen(cmd, cwd=HERE, env=env)
            try:
                wait_port(proc)
                time.sleep(1.0)  # let every worker finish importing
                statuses, workers = fire()
            finally:
                proc.terminate()
                proc.wait(timeout=30)
                if storage.startswith("velocity"):
                    velocity_limits.remove(name)
            ok = statuses.get(200, 0)
            verdict = "correct" if ok == 100 else f"WRONG: {ok / 100:.1f}x the limit"
            print(f"  {label:<28}{storage.split(':')[0] + '://':<12} let through {ok:>4}, refused "
                  f"{statuses.get(429, 0):>4}, other {sum(v for k, v in statuses.items() if k not in (200, 429))}"
                  f"  workers that served: {len(workers)}  {verdict}", flush=True)


if __name__ == "__main__":
    main()
