"""Tests for velocity_limits: the shared-memory table and its `limits` storage backend.

Run from the velocity-limits folder after `python setup.py build_ext --inplace`, or against an
installed wheel from any folder holding only tests/:  python -m unittest discover -s tests -v
The `limits` tests are skipped when limits is not installed. Multi-process tests start fully
independent interpreters (as gunicorn and uvicorn workers are), not multiprocessing children.
"""
import contextlib
import faulthandler
import io
import itertools
import multiprocessing as mp
import os
import random
import struct
import subprocess
import sys
import textwrap
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import velocity_limits  # noqa: E402
from velocity_limits import SharedTable, TableFull, _table  # noqa: E402

try:
    import limits
    from limits import RateLimitItemPerHour, RateLimitItemPerSecond
    from limits.storage import MemoryStorage, storage_from_string
    from limits.strategies import FixedWindowRateLimiter
except ImportError:
    limits = None

_names = itertools.count()
faulthandler.dump_traceback_later(600, exit=True)  # a hang inside C (GIL held) aborts the run


def _workers(code, n, *args, timeout=120):
    """Run ``code`` in n independent interpreters, started together; return their stdout lines."""
    start_at = time.time() + 1.5
    prog = textwrap.dedent(f"""
        import sys, time
        sys.path.insert(0, {ROOT!r})
        args = sys.argv[1:]
        def wait_start():
            time.sleep(max(0.0, {start_at!r} - time.time()))
    """) + textwrap.dedent(code)
    procs = [subprocess.Popen([sys.executable, "-c", prog, *map(str, args)], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) for _ in range(n)]
    out = []
    for p in procs:
        o, e = p.communicate(timeout=timeout)
        if p.returncode != 0:
            raise AssertionError(f"worker exited {p.returncode}: {e}")
        out.append(o.strip())
    return out


def _child_incr(table, key, n):
    for _ in range(n):
        table.incr(key, 3600)


class TableTest(unittest.TestCase):
    def setUp(self):
        self.names = []

    def tearDown(self):
        for name in self.names:
            velocity_limits.remove(name)

    def name(self):
        n = f"t{os.getpid()}-{next(_names)}"
        velocity_limits.remove(n)
        self.names.append(n)
        return n

    def test_installed_package_ships_no_c_source(self):
        pkg = os.path.dirname(velocity_limits.__file__)
        if os.path.dirname(pkg) == ROOT:
            self.skipTest("running against the source tree, not an installed wheel")
        self.assertEqual([f for f in os.listdir(pkg) if f.endswith((".c", ".h"))], [])

    def test_fixed_window_semantics(self):
        with SharedTable(self.name(), 64) as t:
            self.assertEqual([t.incr("a", 0.6), t.incr("a", 0.6), t.incr("a", 0.6, amount=3)], [1, 2, 5])
            self.assertEqual((t.get("a"), t.get("missing")), (5, 0))
            left = t.get_expiry("a") - time.time()
            self.assertTrue(0.3 < left <= 0.61, left)
            self.assertAlmostEqual(t.get_expiry("missing"), time.time(), delta=0.1)
            time.sleep(0.65)
            self.assertEqual(t.get("a"), 0)
            self.assertEqual(t.incr("a", 0.6), 1)  # a new window starts at the new amount

    def test_clear_and_reset(self):
        with SharedTable(self.name(), 64) as t:
            for k in "abc":
                t.incr(k, 60, 2)
            t.clear("b")
            self.assertEqual([t.get(k) for k in "abc"], [2, 0, 2])
            self.assertEqual(t.incr("b", 60), 1)
            self.assertEqual(t.reset(), 3)
            self.assertEqual([t.get(k) for k in "abc"], [0, 0, 0])
            self.assertEqual(t.stats()[2], 0)

    def test_many_keys_are_independent(self):
        with SharedTable(self.name(), 65_536) as t:
            n = 20_000
            for k in range(n):
                for _ in range(k % 7 + 1):
                    t.incr(f"user-{k}", 600)
            wrong = [(k, t.get(f"user-{k}")) for k in range(n) if t.get(f"user-{k}") != k % 7 + 1]
            self.assertEqual(wrong[:5], [], f"{len(wrong)} of {n} keys have the wrong count")  # a 20,000-item diff takes minutes
            self.assertEqual(t.stats()[2], n)

    def test_full_table_raises_then_reuses_expired_slots(self):
        with SharedTable(self.name(), 64) as t:
            with self.assertRaises(TableFull):
                for k in range(65):  # 65 live keys cannot fit in 64 slots
                    t.incr(f"k{k}", 0.3)
            time.sleep(0.35)
            for k in range(24):  # every window has ended: their slots are taken over
                self.assertEqual(t.incr(f"new{k}", 60, 4), 4)
            self.assertEqual([t.get(f"k{k}") for k in range(65)], [0] * 65)

    def test_taken_over_slots_start_clean(self):
        rng = random.Random(7)
        with SharedTable(self.name(), 64) as t:
            for rnd in range(8):
                keys = [f"r{rnd}-{i}" for i in range(30)]
                want = {k: rng.randint(1, 50) for k in keys}
                for k, a in want.items():
                    self.assertEqual(t.incr(k, 0.3, a), a)  # never inherits an old key's count
                self.assertEqual({k: t.get(k) for k in keys}, want)
                time.sleep(0.35)
            self.assertLessEqual(t.stats()[1], 64)

    def test_argument_checks(self):
        with SharedTable(self.name(), 64) as t:
            for bad in (0, -1, float("nan"), float("inf")):
                with self.assertRaises(ValueError):
                    t.incr("k", bad)
            for bad in (-1, _table.MAX_COUNT + 1):
                with self.assertRaises(ValueError):
                    t.incr("k", 1, bad)
            with self.assertRaises(TypeError):
                t.incr(b"k", 1)
            with self.assertRaises(TypeError):
                t.incr("k", 1, cost=2)
            with self.assertRaises(TypeError):
                t.incr("k", 1, 2, amount=2)
            self.assertEqual(t.incr("k", 1, amount=2), 2)

    def test_count_saturates_at_max(self):
        with SharedTable(self.name(), 64) as t:
            t.incr("k", 60, _table.MAX_COUNT)
            self.assertEqual(t.incr("k", 60), _table.MAX_COUNT)

    def test_names_and_capacity_are_validated(self):
        for bad in ("", "../x", "a/b", "x" * 65, 5):
            with self.assertRaises(ValueError):
                SharedTable(bad)
        for bad in (32, 100, 2 ** 31):
            with self.assertRaises(ValueError):
                SharedTable(self.name(), bad)

    def test_capacity_mismatch_is_refused(self):
        name = self.name()
        with SharedTable(name, 64):
            with self.assertRaisesRegex(ValueError, "capacity 64"):
                SharedTable(name, 128)

    def test_closed_table_raises(self):
        t = SharedTable(self.name(), 64)
        incr = t.incr
        t.close()
        with self.assertRaisesRegex(ValueError, "closed"):
            incr("k", 1)
        t.close()  # closing twice is fine

    def test_counts_are_exact_across_independent_processes(self):
        name = self.name()
        with SharedTable(name, 1024) as t:
            out = _workers("""
                from velocity_limits import SharedTable
                t = SharedTable(args[0], 1024)
                keys = [f"key-{i}" for i in range(50)]
                wait_start()
                for i in range(int(args[1])):
                    t.incr(keys[i % 50], 3600)
                print("ok")
            """, 4, name, 20_000)
            self.assertEqual(out, ["ok"] * 4)
            self.assertEqual([t.get(f"key-{i}") for i in range(50)], [4 * 20_000 // 50] * 50)
            self.assertEqual(t.stats()[1], 50)  # concurrent first hits never inserted a key twice

    def test_new_keys_raced_by_many_processes_are_inserted_once(self):
        name = self.name()
        with SharedTable(name, 4096) as t:
            _workers("""
                from velocity_limits import SharedTable
                t = SharedTable(args[0], 4096)
                wait_start()
                for i in range(1500):
                    t.incr(f"burst-{i}", 3600)  # every worker hits the same new keys at once
            """, 6, name)
            self.assertEqual(t.stats()[1], 1500)
            self.assertEqual({t.get(f"burst-{i}") for i in range(1500)}, {6})

    def test_counts_survive_every_process_closing_the_table(self):
        # On Windows a named mapping vanished with its last handle, so workers that started after
        # the others had exited counted from zero again (found by cibuildwheel under load).
        name = self.name()
        with SharedTable(name, 64) as t:
            t.incr("k", 60, 5)
        out = _workers("""
            from velocity_limits import SharedTable
            print(SharedTable(args[0], 64).incr("k", 60))
        """, 1, name)
        self.assertEqual(out, ["6"])

    @unittest.skipUnless("fork" in mp.get_all_start_methods(), "needs fork")
    def test_forked_child_shares_the_table(self):
        with SharedTable(self.name(), 64) as t:
            p = mp.get_context("fork").Process(target=_child_incr, args=(t, "k", 1000))
            p.start()
            p.join(60)
            self.assertEqual((p.exitcode, t.get("k")), (0, 1000))

    def test_spawned_child_reopens_by_name(self):
        with SharedTable(self.name(), 64) as t:
            p = mp.get_context("spawn").Process(target=_child_incr, args=(t, "k", 1000))
            p.start()
            p.join(60)
            self.assertEqual((p.exitcode, t.get("k")), (0, 1000))

    def _dead_pid(self):
        p = subprocess.Popen([sys.executable, "-c", "pass"])
        p.wait()
        return p.pid

    def _lock_word(self, t, key):
        return _table.LOCKS_OFFSET + 8 * t._t._lock_index(key)

    def test_lock_left_by_a_dead_process_is_taken_over(self):
        with SharedTable(self.name(), 64) as t:
            off = self._lock_word(t, "k")
            struct.pack_into("<Q", t._mm, off, self._dead_pid())
            t0 = time.monotonic()
            self.assertEqual(t.incr("k", 60), 1)
            self.assertLess(time.monotonic() - t0, 0.5)
            self.assertEqual(struct.unpack_from("<Q", t._mm, off)[0], 0)

    def test_lock_held_by_a_stalled_live_process_is_taken_over_after_a_second(self):
        sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            with SharedTable(self.name(), 64) as t:
                off = self._lock_word(t, "k")
                struct.pack_into("<Q", t._mm, off, sleeper.pid)
                t0 = time.monotonic()
                self.assertEqual(t.incr("k", 60), 1)
                self.assertTrue(0.9 <= time.monotonic() - t0 < 5)
                self.assertEqual(struct.unpack_from("<Q", t._mm, off)[0], 0)
        finally:
            sleeper.kill()
            sleeper.wait()

    def test_workers_killed_mid_update_leave_the_table_usable(self):
        name = self.name()
        prog = textwrap.dedent(f"""
            import sys
            sys.path.insert(0, {ROOT!r})
            from velocity_limits import SharedTable
            t = SharedTable({name!r}, 256)
            i = 0
            while True:
                try:
                    t.incr(f"churn-{{i % 400}}", 0.02)
                except Exception:
                    pass  # TableFull while 400 keys churn through 256 slots
                i += 1
        """)
        with SharedTable(name, 256) as t:
            for _ in range(10):
                ps = [subprocess.Popen([sys.executable, "-c", prog]) for _ in range(3)]
                time.sleep(0.4)
                for p in ps:
                    p.kill()  # SIGKILL on POSIX, TerminateProcess on Windows: at any instruction
                for p in ps:
                    p.wait()
            time.sleep(0.05)
            for k in range(100):  # no slot or lock is left stuck
                self.assertEqual(t.incr(f"after-{k}", 60, 3), 3)


@unittest.skipIf(limits is None, "limits is not installed")
class LimitsStorageTest(unittest.TestCase):
    def setUp(self):
        self.names, self.storages = [], []
        self.name = self.new_name()

    def tearDown(self):
        for s in self.storages:  # Windows cannot delete a file that is still mapped
            s.close()
        for name in self.names:
            velocity_limits.remove(name)

    def new_name(self):
        name = f"s{os.getpid()}-{next(_names)}"
        velocity_limits.remove(name)
        self.names.append(name)
        return name

    def storage(self, query="", name=None, **options):
        s = storage_from_string(f"velocity://{name or self.name}{query}", **options)
        self.storages.append(s)
        return s

    def test_uri_selects_this_storage(self):
        s = self.storage("?capacity=128")
        self.assertIsInstance(s, velocity_limits.VelocityStorage)
        self.assertEqual(s._table.capacity, 128)
        self.assertTrue(s.check())
        s.close()
        self.assertFalse(s.check())

    def test_same_answers_as_memory_storage(self):
        ours, ref = self.storage(), MemoryStorage()
        rng = random.Random(3)
        for _ in range(3000):
            key, op = f"k{rng.randrange(40)}", rng.random()
            if op < 0.7:
                amount = rng.randint(1, 5)
                self.assertEqual(ours.incr(key, 600, amount=amount), ref.incr(key, 600, amount=amount))
            elif op < 0.95:
                self.assertEqual(ours.get(key), ref.get(key))
            else:
                ours.clear(key)
                ref.clear(key)
        self.assertEqual(ours.reset(), ref.reset())

    def test_fixed_window_limiter(self):
        for wrap in (False, True):
            with self.subTest(wrap_exceptions=wrap):
                lim = FixedWindowRateLimiter(self.storage(name=self.new_name(), wrap_exceptions=wrap))
                item = RateLimitItemPerSecond(2)
                self.assertEqual([lim.hit(item, "u") for _ in range(3)], [True, True, False])
                self.assertFalse(lim.test(item, "u"))
                reset, remaining = lim.get_window_stats(item, "u")
                self.assertEqual(remaining, 0)
                self.assertTrue(time.time() < reset <= time.time() + 1.01)
                lim.clear(item, "u")
                self.assertTrue(lim.test(item, "u"))

    def test_table_full_is_wrapped_when_asked(self):
        s = self.storage("?capacity=64", wrap_exceptions=True)
        with self.assertRaises(limits.errors.StorageError):
            for k in range(65):
                s.incr(f"k{k}", 60)

    def test_limit_holds_across_independent_workers(self):
        out = _workers("""
            import velocity_limits
            from limits import RateLimitItemPerHour
            from limits.storage import storage_from_string
            from limits.strategies import FixedWindowRateLimiter
            lim = FixedWindowRateLimiter(storage_from_string("velocity://" + args[0]))
            item = RateLimitItemPerHour(1000)
            wait_start()
            print(sum(lim.hit(item, "client-1") for _ in range(1000)))
        """, 4, self.name)
        self.assertEqual(sum(map(int, out)), 1000)

    def test_demo_shows_memory_overcounting_and_an_exact_shared_limit(self):
        from velocity_limits import demo
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            status = demo.main(["--workers", "3", "--requests", "300", "--limit", "50"])
        rows = {line.split()[0]: line.split()[1:3] for line in buf.getvalue().splitlines()
                if line.strip().startswith(("memory://", "velocity://"))}
        self.assertEqual(status, 0, buf.getvalue())
        self.assertEqual(rows, {"memory://": ["150", "50"], "velocity://": ["50", "50"]})
        self.assertFalse(os.path.exists(velocity_limits.path(f"demo-{os.getpid()}")))


if __name__ == "__main__":
    unittest.main()
