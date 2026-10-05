"""A pilot: ``python -m graphed_executors.htcondor_backend.pilot <url> <secret file>``.

It registers with the driver's task server, beats from a daemon thread, and runs leased tasks one at a
time until the server answers 410. It exits 0 when the driver is done with it and 2 when the driver
refuses its signature. When the driver has been unreachable for a lease it sends itself SIGTERM, even in
the middle of a task, so an orphan frees its batch slot.
"""

from __future__ import annotations

import os
import pickle
import signal
import socket
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid
from pathlib import Path

from . import server


class _Driver:
    def __init__(self, url: str, key: bytes) -> None:
        self.url = url.rstrip("/")
        self.key = key
        self.lease_s = server.LEASE_S
        self.last_ok = time.monotonic()

    def post(self, route: str, obj: object) -> tuple[int, bytes, str | None]:
        """POST ``obj`` signed; retries while the driver is unreachable for less than a lease."""
        body = pickle.dumps(obj)
        headers = {server.SIG_HEADER: server.sign(self.key, body)}
        while True:
            request = urllib.request.Request(self.url + route, data=body, method="POST", headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=3 * server.POLL_S) as response:
                    status, data, tid = (
                        response.status,
                        response.read(),
                        response.headers.get(server.TASK_HEADER),
                    )
            except urllib.error.HTTPError as exc:
                status, data, tid = exc.code, b"", None
            except OSError as exc:
                if time.monotonic() - self.last_ok > self.lease_s:
                    print(f"driver {self.url} unreachable for {self.lease_s}s: {exc}", flush=True)
                    # from either thread, mid-task too; a signal (not os._exit) lets coverage save its data
                    os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(1.0)
                continue
            self.last_ok = time.monotonic()
            if status == 403:
                print("driver refused the pilot's signature: wrong secret file", flush=True)
                sys.exit(2)
            return int(status), data, tid


def _run(data: bytes) -> tuple[bool, bytes]:
    """Run a leased ``(shim, fn, args)``; any failure comes back as a picklable exception."""
    try:
        shim, fn, args = pickle.loads(data)
        return True, pickle.dumps(shim(fn, *args))
    except Exception as exc:
        try:
            return False, pickle.dumps(exc)
        except Exception:
            return False, pickle.dumps(RuntimeError(f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"))


def _beat(driver: _Driver, me: str, beat_s: float) -> None:
    status = 200
    while status != 410:
        time.sleep(beat_s)
        status, _, _ = driver.post("/beat", me)


def main(argv: list[str]) -> int:
    url, secret_path = argv
    driver = _Driver(url, bytes.fromhex(Path(secret_path).read_text().strip()))
    # hostname:pid alone collides when containers number their own pids
    me = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    _, data, _ = driver.post("/hello", me)
    conf = pickle.loads(data)
    driver.lease_s = conf["lease_s"]
    print(f"pilot {me} serving {driver.url}", flush=True)
    threading.Thread(target=_beat, args=(driver, me, conf["beat_s"]), daemon=True).start()
    while True:
        status, data, tid = driver.post("/next", me)
        if status == 410:
            return 0
        if status == 200:
            driver.post("/result", (me, int(tid or -1), *_run(data)))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
