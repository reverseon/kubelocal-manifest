#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import random
import string
import sys
import time
from typing import List, Sequence, Tuple

# Expected environment variables (os.environ):
# - REDISCLI_AUTH (required): Redis password (also used as Sentinel password in this repo)
# - REDIS_SENTINELS (optional): comma-separated Sentinel endpoints "host:port"
#   - default: redis-sentinel.sidekiq-sandbox.svc.cluster.local:26379
# - REDIS_SENTINEL_MASTER_NAME (optional): Sentinel master name
#   - default: cluster_name
# - REDIS_DB (optional): Redis database index (int)
#   - default: 0
# - REDIS_PIPELINE_BATCH (optional): commands per pipeline execute (int)
#   - default: 1000
# - REDIS_TIMEOUT_S (optional): socket timeout seconds (float)
#   - default: 3.0
# How to use
# python populate.py <key_prefix> <key_count>

try:
    from redis.exceptions import RedisError
    from redis.sentinel import Sentinel
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: redis-py.\n"
        "Install it with: pip install redis\n"
        f"Import error: {e!r}"
    )


def _die(msg: str, code: int = 2) -> None:
    sys.stderr.write(msg.rstrip() + "\n")
    raise SystemExit(code)


def parse_sentinels(spec: str, default_port: int = 26379) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for item in (x.strip() for x in spec.split(",") if x.strip()):
        if ":" in item:
            host, port_s = item.rsplit(":", 1)
            out.append((host, int(port_s)))
        else:
            out.append((item, default_port))
    return out


def rand_ascii(n: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def chunked(seq: Sequence[Sequence[str]], size: int) -> Sequence[Sequence[Sequence[str]]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]

def _safe_int(x: object, default: int = 0) -> int:
    try:
        return int(x)  # type: ignore[arg-type]
    except Exception:
        return default


def collect_stats(r) -> dict:
    """
    Best-effort stats snapshot (keys + memory + ops).
    Uses Redis INFO and DBSIZE; missing fields default to 0/empty.
    """
    info_all = {}
    try:
        info_all = r.info()
    except Exception:
        info_all = {}

    mem = info_all.get("used_memory", 0)
    mem_h = info_all.get("used_memory_human", "")
    mem_peak = info_all.get("used_memory_peak", 0)
    mem_peak_h = info_all.get("used_memory_peak_human", "")
    frag = info_all.get("mem_fragmentation_ratio", "")

    total_cmds = info_all.get("total_commands_processed", 0)
    ops_sec = info_all.get("instantaneous_ops_per_sec", 0)

    keys = 0
    try:
        keys = int(r.dbsize())
    except Exception:
        keys = 0

    return {
        "dbsize": _safe_int(keys),
        "used_memory": _safe_int(mem),
        "used_memory_human": str(mem_h) if mem_h is not None else "",
        "used_memory_peak": _safe_int(mem_peak),
        "used_memory_peak_human": str(mem_peak_h) if mem_peak_h is not None else "",
        "mem_fragmentation_ratio": frag,
        "total_commands_processed": _safe_int(total_cmds),
        "instantaneous_ops_per_sec": _safe_int(ops_sec),
    }


def main(argv: Sequence[str]) -> int:
    if len(argv) < 3:
        _die("usage: populate.py <key_prefix> <key_count>")

    key_prefix = argv[1]
    try:
        key_count = int(argv[2])
    except ValueError:
        _die("key_count must be an integer")
    if key_count < 1:
        _die("key_count must be >= 1")

    password = os.environ.get("REDISCLI_AUTH", "")
    if not password:
        _die("env var REDISCLI_AUTH must be set (used as Redis + Sentinel password)")

    sentinel_hosts = os.environ.get(
        "REDIS_SENTINELS",
        "redis-sentinel.sidekiq-sandbox.svc.cluster.local:26379",
    )
    master_name = os.environ.get("REDIS_SENTINEL_MASTER_NAME", "cluster_name")
    db = int(os.environ.get("REDIS_DB", "0"))
    batch_size = int(os.environ.get("REDIS_PIPELINE_BATCH", "1000"))
    timeout_s = float(os.environ.get("REDIS_TIMEOUT_S", "3.0"))

    sentinels = parse_sentinels(sentinel_hosts)

    sentinel = Sentinel(
        sentinels,
        socket_timeout=timeout_s,
        decode_responses=True,
        sentinel_kwargs={"password": password, "socket_timeout": timeout_s, "decode_responses": True},
    )

    # Use sentinel-discovered master; auth with same password.
    r = sentinel.master_for(
        master_name,
        password=password,
        db=db,
        socket_timeout=timeout_s,
        decode_responses=True,
    )

    try:
        r.ping()
    except RedisError as e:
        _die(f"failed to connect/ping Redis master via Sentinel ({master_name}): {e!r}", code=3)

    stats_before = collect_stats(r)
    t0 = time.time()
    now_ms = int(time.time() * 1000)

    # Exactly key_count keys, mixed types:
    # - ~50% flat string keys (SET)
    # - ~35% "nested" namespaced hash keys (HSET)
    # - ~15% list keys (RPUSH)
    n_set = key_count // 2
    n_hset = (key_count - n_set) * 7 // 10
    n_list = key_count - n_set - n_hset

    commands: List[Sequence[str]] = []

    for i in range(n_set):
        k = f"{key_prefix}{i}"
        v = json.dumps(
            {"kind": "flat", "i": i, "ts_ms": now_ms, "payload": rand_ascii(128)},
            separators=(",", ":"),
        )
        commands.append(("SET", k, v))

    for i in range(n_hset):
        k = f"{key_prefix}:ns:{i}"
        meta = json.dumps(
            {"kind": "hash", "i": i, "ts_ms": now_ms, "payload": rand_ascii(64)},
            separators=(",", ":"),
        )
        commands.append(("HSET", k, "a", rand_ascii(16), "b", str(i), "meta", meta))

    for i in range(n_list):
        k = f"{key_prefix}:list:{i}"
        commands.append(("RPUSH", k, rand_ascii(12), rand_ascii(12), rand_ascii(12), rand_ascii(12), rand_ascii(12)))

    random.shuffle(commands)

    done = 0
    for batch in chunked(commands, max(1, batch_size)):
        pipe = r.pipeline(transaction=False)
        for cmd in batch:
            pipe.execute_command(*cmd)
        pipe.execute()

        done += len(batch)
        if done == len(commands) or done % 10_000 == 0:
            sys.stderr.write(f"written {done}/{len(commands)} keys\n")

    dt = time.time() - t0
    stats_after = collect_stats(r)

    sys.stderr.write(f"done: wrote {len(commands)} keys in {dt:.2f}s (db={db}, master_name={master_name})\n")
    sys.stderr.write(
        "stats:\n"
        f"  dbsize: {stats_before['dbsize']} -> {stats_after['dbsize']} (delta {stats_after['dbsize'] - stats_before['dbsize']})\n"
        f"  used_memory: {stats_before['used_memory']} -> {stats_after['used_memory']} (delta {stats_after['used_memory'] - stats_before['used_memory']})\n"
        f"  used_memory_human: {stats_before['used_memory_human']} -> {stats_after['used_memory_human']}\n"
        f"  used_memory_peak_human: {stats_after['used_memory_peak_human']}\n"
        f"  mem_fragmentation_ratio: {stats_after['mem_fragmentation_ratio']}\n"
        f"  total_commands_processed: {stats_before['total_commands_processed']} -> {stats_after['total_commands_processed']} (delta {stats_after['total_commands_processed'] - stats_before['total_commands_processed']})\n"
        f"  instantaneous_ops_per_sec: {stats_after['instantaneous_ops_per_sec']}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


