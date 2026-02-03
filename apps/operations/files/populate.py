#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import random
import secrets
import string
import sys
import time
from typing import List, Sequence, Tuple

# Expected environment variables (os.environ):
# - REDISCLI_AUTH (required): Redis password (also used as Sentinel password in this repo)
# - REDIS_URL (optional): Direct Redis connection URL (e.g., redis://host:port or redis://host:port/db)
#   - If set, direct connection is used instead of Sentinel
# - REDIS_HOST (optional): Redis host (used if REDIS_URL not set and for direct connection)
# - REDIS_PORT (optional): Redis port (default: 6379, used with REDIS_HOST for direct connection)
# - REDIS_SENTINELS (optional): comma-separated Sentinel endpoints "host:port"
#   - default: redis-sentinel.sidekiq-sandbox.svc.cluster.local:26379
#   - Used only if REDIS_URL and REDIS_HOST are not set
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
    import redis
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

    db = int(os.environ.get("REDIS_DB", "0"))
    batch_size = int(os.environ.get("REDIS_PIPELINE_BATCH", "1000"))
    timeout_s = float(os.environ.get("REDIS_TIMEOUT_S", "3.0"))

    redis_url = os.environ.get("REDIS_URL", "")
    redis_host = os.environ.get("REDIS_HOST", "")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))

    # Connection mode selection:
    # 1. REDIS_URL takes precedence (direct connection via URL)
    # 2. REDIS_HOST uses direct connection with host:port
    # 3. Otherwise use Sentinel (backward compatible)
    
    if redis_url:
        # Direct connection via URL
        try:
            connection_kwargs = {
                "password": password,
                "db": db,
                "socket_timeout": timeout_s,
                "decode_responses": True,
            }
            # Only add ssl_cert_reqs if using SSL (rediss://)
            if redis_url.startswith("rediss://"):
                connection_kwargs["ssl_cert_reqs"] = None
            
            r = redis.from_url(redis_url, **connection_kwargs)
            r.ping()
        except RedisError as e:
            _die(f"failed to connect/ping Redis via URL ({redis_url}): {e!r}", code=3)
        master_name = "direct"
    elif redis_host:
        # Direct connection via host:port
        try:
            connection_kwargs = {
                "host": redis_host,
                "port": redis_port,
                "password": password,
                "db": db,
                "socket_timeout": timeout_s,
                "decode_responses": True,
            }
            # Check if SSL should be used
            use_ssl = os.environ.get("REDIS_SSL", "").lower() in ("true", "1", "yes")
            if use_ssl:
                connection_kwargs["ssl"] = True
                connection_kwargs["ssl_cert_reqs"] = None
            
            r = redis.Redis(**connection_kwargs)
            r.ping()
        except RedisError as e:
            _die(f"failed to connect/ping Redis at {redis_host}:{redis_port}: {e!r}", code=3)
        master_name = "direct"
    else:
        # Sentinel connection (backward compatible)
        sentinel_hosts = os.environ.get(
            "REDIS_SENTINELS",
            "redis-sentinel.sidekiq-sandbox.svc.cluster.local:26379",
        )
        master_name = os.environ.get("REDIS_SENTINEL_MASTER_NAME", "cluster_name")
        
        sentinels = parse_sentinels(sentinel_hosts)
        
        # Check if SSL should be used
        use_ssl = os.environ.get("REDIS_SSL", "").lower() in ("true", "1", "yes")
        
        sentinel_kwargs = {
            "password": password,
            "socket_timeout": timeout_s,
            "decode_responses": True,
        }
        
        sentinel_init_kwargs = {
            "socket_timeout": timeout_s,
            "decode_responses": True,
        }
        
        master_kwargs = {
            "password": password,
            "db": db,
            "socket_timeout": timeout_s,
            "decode_responses": True,
        }
        
        # Only add SSL params if SSL is enabled
        if use_ssl:
            sentinel_kwargs["ssl"] = True
            sentinel_kwargs["ssl_cert_reqs"] = None
            sentinel_init_kwargs["ssl"] = True
            sentinel_init_kwargs["ssl_cert_reqs"] = None
            master_kwargs["ssl"] = True
            master_kwargs["ssl_cert_reqs"] = None
        
        sentinel = Sentinel(
            sentinels,
            **sentinel_init_kwargs,
            sentinel_kwargs=sentinel_kwargs,
        )
        
        # Use sentinel-discovered master; auth with same password.
        r = sentinel.master_for(master_name, **master_kwargs)
        
        try:
            r.ping()
        except RedisError as e:
            _die(f"failed to connect/ping Redis master via Sentinel ({master_name}): {e!r}", code=3)

    stats_before = collect_stats(r)
    t0 = time.time()
    now_ms = int(time.time() * 1000)

    # Generate exactly N keys:
    # - key name: <prefix><delimiter><random_suffix>
    # - data: arbitrary payload; mixed types (SET/HSET/LIST)
    #
    # NOTE: Use secrets (not random) for suffixes to avoid collisions and to be
    # truly unpredictable.
    delim = "" if key_prefix.endswith(":") else ":"

    # Exactly key_count keys, mixed types:
    # - ~50% flat string keys (SET)
    # - ~35% hash keys (HSET)
    # - ~15% list keys (RPUSH)
    n_set = key_count // 2
    n_hset = (key_count - n_set) * 7 // 10
    n_list = key_count - n_set - n_hset

    written = 0
    pipe = r.pipeline(transaction=False)
    try:
        # SET keys
        for i in range(n_set):
            suffix = secrets.token_urlsafe(18)
            key = f"{key_prefix}{delim}{suffix}"
            value = json.dumps(
                {
                    "kind": "string",
                    "ts_ms": now_ms,
                    "i": i,
                    "data": secrets.token_urlsafe(48),
                },
                separators=(",", ":"),
            )
            pipe.set(key, value)
            # Randomly set TTL: 50% chance of TTL between 7200-14400 seconds
            if random.choice([True, False]):
                pipe.expire(key, random.randint(7200, 14400))
            written += 1

            if written % batch_size == 0:
                pipe.execute()

        # HSET keys
        for i in range(n_hset):
            suffix = secrets.token_urlsafe(18)
            key = f"{key_prefix}{delim}{suffix}"
            mapping = {
                "kind": "hash",
                "ts_ms": str(now_ms),
                "i": str(i),
                "data": secrets.token_urlsafe(48),
                "rand": str(random.randint(0, 1_000_000_000)),
            }
            pipe.hset(key, mapping=mapping)
            # Randomly set TTL: 50% chance of TTL between 7200-14400 seconds
            if random.choice([True, False]):
                pipe.expire(key, random.randint(7200, 14400))
            written += 1

            if written % batch_size == 0:
                pipe.execute()

        # LIST keys
        for i in range(n_list):
            suffix = secrets.token_urlsafe(18)
            key = f"{key_prefix}{delim}{suffix}"
            items = [
                json.dumps(
                    {"kind": "list_item", "ts_ms": now_ms, "i": i, "data": secrets.token_urlsafe(24)},
                    separators=(",", ":"),
                )
                for _ in range(3)
            ]
            pipe.rpush(key, *items)
            # Randomly set TTL: 50% chance of TTL between 7200-14400 seconds
            if random.choice([True, False]):
                pipe.expire(key, random.randint(7200, 14400))
            written += 1

            if written % batch_size == 0:
                pipe.execute()

        # Flush remaining commands.
        if written % batch_size != 0:
            pipe.execute()
    except RedisError as e:
        _die(f"failed while writing keys (written={written}): {e!r}", code=4)

    dt = time.time() - t0
    stats_after = collect_stats(r)

    print(f"done: wrote {written} keys in {dt:.2f}s (db={db}, master_name={master_name})", file=sys.stderr, flush=True)
    print(
        "stats:\n"
        f"  dbsize: {stats_before['dbsize']} -> {stats_after['dbsize']} (delta {stats_after['dbsize'] - stats_before['dbsize']})\n"
        f"  used_memory: {stats_before['used_memory']} -> {stats_after['used_memory']} (delta {stats_after['used_memory'] - stats_before['used_memory']})\n"
        f"  used_memory_human: {stats_before['used_memory_human']} -> {stats_after['used_memory_human']}\n"
        f"  used_memory_peak_human: {stats_after['used_memory_peak_human']}\n"
        f"  mem_fragmentation_ratio: {stats_after['mem_fragmentation_ratio']}\n"
        f"  total_commands_processed: {stats_before['total_commands_processed']} -> {stats_after['total_commands_processed']} (delta {stats_after['total_commands_processed'] - stats_before['total_commands_processed']})\n"
        f"  instantaneous_ops_per_sec: {stats_after['instantaneous_ops_per_sec']}\n",
        file=sys.stderr,
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


