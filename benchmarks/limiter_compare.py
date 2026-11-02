"""Rate limits across worker processes on one server: today's free options and velocity_limits.

Three questions:
  1. Cost of one rate-limit check, one process, through the `limits` library (the one under
     Flask-Limiter and slowapi) on per-process memory, on Redis, and on velocity_limits; and
     pyrate-limiter's SQLite bucket with a file lock. The bare velocity_limits table call (no
     `limits` in between) is listed too.
  2. Correctness across independently started workers, the way gunicorn/uvicorn run them:
     WORKERS separate interpreters, each tries ATTEMPTS checks on one key whose limit is LIMIT per
     hour. A correct limiter allows exactly LIMIT in total.
  3. Throughput: BUSY workers checking 1,000 different keys at once, total checks per second.

Linux only, with redis-server on PATH. Run:
    pip install "velocity-limits[limits]" redis pyrate-limiter "filelock<4"
    python3 benchmarks/limiter_compare.py
"""
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from importlib.metadata import version

RUNS, WORKERS, ATTEMPTS, LIMIT, BUSY, BUSY_CHECKS = 5, 4, 1000, 1000, 8, 20_000


def start_redis(tmp):
    """A private redis-server on port 6390 and a unix socket in tmp; returns (process, port, socket path)."""
    import redis
    sock = os.path.join(tmp, "redis.sock")
    port = 6390
    proc = subprocess.Popen(
        ["redis-server", "--port", str(port), "--unixsocket", sock, "--unixsocketperm", "700",
         "--save", "", "--appendonly", "no", "--daemonize", "no", "--dir", tmp],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            redis.Redis(port=port).ping()
            return proc, port, sock
        except redis.ConnectionError:
            time.sleep(0.05)
    raise RuntimeError("redis-server did not start")


def make_check(backend, where, limit, key="user-1"):
    """Return check(key=...) -> bool; where is the Redis socket, SQLite file or table name."""
    if backend in ("limits-memory", "limits-redis", "limits-velocity"):
        from limits import RateLimitItemPerHour
        from limits.storage import MemoryStorage, RedisStorage, storage_from_string
        from limits.strategies import FixedWindowRateLimiter
        if backend == "limits-memory":
            storage = MemoryStorage()
        elif backend == "limits-redis":
            storage = RedisStorage(f"redis+unix://{where}")
        else:
            import velocity_limits  # noqa: F401  registers velocity://
            storage = storage_from_string(f"velocity://{where}")
        limiter, item = FixedWindowRateLimiter(storage), RateLimitItemPerHour(limit)
        return lambda k=key: limiter.hit(item, k)
    if backend == "pyrate-sqlite":
        from pyrate_limiter import Duration, Limiter, Rate, SQLiteBucket
        bucket = SQLiteBucket.init_from_file([Rate(limit, Duration.HOUR)], db_path=where, use_file_lock=True)
        limiter = Limiter(bucket)
        return lambda k=key: limiter.try_acquire(k, blocking=False)
    raise ValueError(backend)


def worker(mode, backend, where, start_at):
    if mode == "limit":
        check = make_check(backend, where, LIMIT)
        time.sleep(max(0.0, start_at - time.time()))
        print(sum(1 for _ in range(ATTEMPTS) if check()))
    else:  # busy: many keys, high limit, report elapsed seconds
        check = make_check(backend, where, 10**6)
        keys = [f"client-{i}" for i in range(1000)]
        time.sleep(max(0.0, start_at - time.time()))
        t0 = time.perf_counter()
        for i in range(BUSY_CHECKS):
            check(keys[i % 1000])
        print(time.perf_counter() - t0)


def spawn(mode, backend, where, n):
    start_at = time.time() + 2.0
    procs = [subprocess.Popen([sys.executable, __file__, "--worker", mode, backend, str(where), str(start_at)],
                              stdout=subprocess.PIPE, text=True) for _ in range(n)]
    return [float(p.communicate()[0]) for p in procs]


def per_check_ns(fn, n):
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter_ns()
        for _ in range(n):
            fn()
        times.append((time.perf_counter_ns() - t0) / n)
    return statistics.median(times), max(times) / min(times)


def main():
    import redis
    import velocity_limits

    cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "?")
    print(f"# {cpu}, {os.cpu_count()} logical CPUs, {platform.release()}")
    print(f"# Python {platform.python_version()}; limits {version('limits')}; pyrate-limiter "
          f"{version('pyrate-limiter')}; filelock {version('filelock')}; redis-py {version('redis')}; "
          f"velocity_limits {velocity_limits.__version__}\n")

    tmp = tempfile.mkdtemp(prefix="lim-")
    rproc, _, rsock = start_redis(tmp)
    tables = [f"bench-{os.getpid()}-{i}" for i in range(4)]
    for t in tables:
        velocity_limits.remove(t)
    try:
        print("== 1. one rate-limit check, one process (limit set high so every check is allowed) ==")
        for backend, where, n in (("limits-memory", None, 200_000),
                                  ("limits-redis", rsock, 20_000),
                                  ("limits-velocity", tables[0], 200_000),
                                  ("pyrate-sqlite", os.path.join(tmp, "speed.db"), 5_000)):
            ns, sp = per_check_ns(make_check(backend, where, 10**9), n)
            print(f"  {backend:<34}{ns:>12,.1f} ns per check   spread {sp:4.2f}x")
        with velocity_limits.SharedTable(tables[1]) as t:
            incr = t.incr
            ns, sp = per_check_ns(lambda: incr("LIMITER/user-1/1000/1/hour", 3600), 2_000_000)
            print(f"  {'velocity_limits table incr() alone':<34}{ns:>12,.1f} ns per check   spread {sp:4.2f}x")

        print(f"\n== 2. {WORKERS} independently started workers, {ATTEMPTS} attempts each, "
              f"limit {LIMIT}/hour on one key ==")
        for backend, where in (("limits-memory", None), ("limits-redis", rsock),
                               ("limits-velocity", tables[2]),
                               ("pyrate-sqlite", os.path.join(tmp, "workers.db"))):
            if backend == "limits-redis":
                redis.Redis(unix_socket_path=rsock).flushall()
            allowed = [int(a) for a in spawn("limit", backend, where, WORKERS)]
            total = sum(allowed)
            verdict = "correct" if total == LIMIT else f"WRONG: {total / LIMIT:.1f}x the limit"
            print(f"  {backend:<34}allowed {total:>5} of {LIMIT}  per worker {allowed}  {verdict}")

        print(f"\n== 3. {BUSY} workers at once, {BUSY_CHECKS:,} checks each over 1,000 keys ==")
        for backend, where in (("limits-redis", rsock), ("limits-velocity", tables[3])):
            if backend == "limits-redis":
                redis.Redis(unix_socket_path=rsock).flushall()
            elapsed = spawn("busy", backend, where, BUSY)
            print(f"  {backend:<34}{BUSY * BUSY_CHECKS / max(elapsed) / 1e3:>10,.1f} k checks/s in total"
                  f"   (slowest worker {max(elapsed):.2f} s)")
    finally:
        rproc.terminate()
        rproc.wait()
        shutil.rmtree(tmp, ignore_errors=True)
        for t in tables:
            velocity_limits.remove(t)


if __name__ == "__main__":
    if len(sys.argv) == 6 and sys.argv[1] == "--worker":
        worker(sys.argv[2], sys.argv[3], sys.argv[4], float(sys.argv[5]))
    else:
        main()
