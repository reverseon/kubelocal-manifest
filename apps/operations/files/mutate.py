#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import random
import secrets
import sys
import time
from typing import List, Tuple

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
# - REDIS_TIMEOUT_S (optional): socket timeout seconds (float)
#   - default: 3.0
# - SCAN_COUNT (optional): SCAN count parameter (int)
#   - default: 1000
# How to use
# python mutate.py [--key-pattern PATTERN]

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


def mutate_string_value(val: str) -> str:
    """
    Mutate a string value by randomly deleting/modifying/adding characters.
    """
    if not val:
        return secrets.token_urlsafe(8)
    
    # Try to parse as JSON and mutate structured data
    try:
        data = json.loads(val)
        if isinstance(data, dict):
            # Modify a random field
            if data:
                key = random.choice(list(data.keys()))
                if isinstance(data[key], str):
                    data[key] = data[key][::-1]  # Reverse string
                elif isinstance(data[key], int):
                    data[key] = data[key] + random.randint(-100, 100)
            return json.dumps(data, separators=(",", ":"))
    except (json.JSONDecodeError, ValueError):
        pass
    
    # For non-JSON strings, do simple mutations
    operations = [
        lambda s: s[1:] if len(s) > 1 else s + "X",  # Delete first char
        lambda s: s[:-1] if len(s) > 1 else s + "Y",  # Delete last char
        lambda s: s[::-1],  # Reverse
        lambda s: s + secrets.token_urlsafe(4),  # Append random
    ]
    return random.choice(operations)(val)


def mutate_value(r: redis.Redis, key: str, typ: str) -> None:
    """
    Mutate the value of a key based on its type.
    """
    if typ == "string":
        val = r.get(key)
        if val is not None:
            mutated = mutate_string_value(val)
            r.set(key, mutated)
    
    elif typ == "hash":
        fields = r.hkeys(key)
        if fields:
            # Modify a random field
            field = random.choice(fields)
            val = r.hget(key, field)
            if val is not None:
                mutated = mutate_string_value(val)
                r.hset(key, field, mutated)
    
    elif typ == "list":
        length = r.llen(key)
        if length > 0:
            # Modify a random list element
            idx = random.randint(0, length - 1)
            val = r.lindex(key, idx)
            if val is not None:
                mutated = mutate_string_value(val)
                r.lset(key, idx, mutated)
    
    elif typ == "set":
        members = list(r.smembers(key))
        if members:
            # Remove a random member and add a new one
            member = random.choice(members)
            r.srem(key, member)
            r.sadd(key, secrets.token_urlsafe(8))
    
    elif typ == "zset":
        members = r.zrange(key, 0, -1)
        if members:
            # Modify score of a random member
            member = random.choice(members)
            r.zincrby(key, random.uniform(-100, 100), member)


def mutate_ttl(r: redis.Redis, key: str) -> None:
    """
    Mutate the TTL of a key.
    Randomly: add TTL, remove TTL, or change existing TTL.
    """
    current_ttl = r.ttl(key)
    
    if current_ttl == -1:
        # Key has no expiry, add one
        new_ttl = random.randint(300, 7200)  # 5 min to 2 hours
        r.expire(key, new_ttl)
    elif current_ttl > 0:
        # Key has expiry, randomly modify or remove
        action = random.choice(["modify", "remove"])
        if action == "modify":
            # Change TTL by ±50%
            delta = int(current_ttl * random.uniform(-0.5, 0.5))
            new_ttl = max(60, current_ttl + delta)  # At least 60s
            r.expire(key, new_ttl)
        else:
            # Remove expiry
            r.persist(key)


def main(argv: List[str]) -> int:
    # Parse optional key pattern argument
    key_pattern = "*"
    if len(argv) > 1:
        if argv[1] == "--key-pattern" and len(argv) > 2:
            key_pattern = argv[2]
        else:
            _die("usage: mutate.py [--key-pattern PATTERN]")
    
    password = os.environ.get("REDISCLI_AUTH", "")
    if not password:
        _die("env var REDISCLI_AUTH must be set (used as Redis + Sentinel password)")

    db = int(os.environ.get("REDIS_DB", "0"))
    timeout_s = float(os.environ.get("REDIS_TIMEOUT_S", "3.0"))
    scan_count = int(os.environ.get("SCAN_COUNT", "1000"))

    redis_url = os.environ.get("REDIS_URL", "")
    redis_host = os.environ.get("REDIS_HOST", "")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))

    # Connection mode selection (same as populate.py)
    if redis_url:
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
        
        r = sentinel.master_for(master_name, **master_kwargs)
        
        try:
            r.ping()
        except RedisError as e:
            _die(f"failed to connect/ping Redis master via Sentinel ({master_name}): {e!r}", code=3)

    print(f"Scanning keys with pattern: {key_pattern}", file=sys.stderr, flush=True)
    
    t0 = time.time()
    processed = 0
    skipped = 0
    mutated_values = 0
    mutated_ttls = 0
    deleted_keys = 0
    
    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=key_pattern, count=scan_count)
            
            for key in keys:
                processed += 1
                
                # 40% chance: do nothing
                # 25% chance: mutate value
                # 25% chance: mutate TTL
                # 10% chance: delete key
                action = random.random()
                
                if action < 0.40:
                    # Do nothing
                    skipped += 1
                elif action < 0.65:
                    # Mutate value
                    try:
                        typ = r.type(key)
                        mutate_value(r, key, typ)
                        mutated_values += 1
                    except RedisError as e:
                        print(f"Warning: failed to mutate value for key '{key}': {e!r}", file=sys.stderr)
                elif action < 0.90:
                    # Mutate TTL
                    try:
                        mutate_ttl(r, key)
                        mutated_ttls += 1
                    except RedisError as e:
                        print(f"Warning: failed to mutate TTL for key '{key}': {e!r}", file=sys.stderr)
                else:
                    # Delete key
                    try:
                        r.delete(key)
                        deleted_keys += 1
                    except RedisError as e:
                        print(f"Warning: failed to delete key '{key}': {e!r}", file=sys.stderr)
                
                # Progress reporting every 1000 keys
                if processed % 1000 == 0:
                    print(
                        f"Progress: processed {processed} keys "
                        f"(skipped={skipped}, value_mutations={mutated_values}, ttl_mutations={mutated_ttls}, deletions={deleted_keys})",
                        file=sys.stderr,
                        flush=True,
                    )
            
            if cursor == 0:
                break
    
    except RedisError as e:
        _die(f"Redis error during scan: {e!r}", code=4)
    
    dt = time.time() - t0
    
    print(
        f"\nDone: processed {processed} keys in {dt:.2f}s (db={db}, master_name={master_name})\n"
        f"  Skipped (no change): {skipped} ({100*skipped/processed:.1f}%)\n"
        f"  Value mutations: {mutated_values} ({100*mutated_values/processed:.1f}%)\n"
        f"  TTL mutations: {mutated_ttls} ({100*mutated_ttls/processed:.1f}%)\n"
        f"  Deletions: {deleted_keys} ({100*deleted_keys/processed:.1f}%)",
        file=sys.stderr,
        flush=True,
    )
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
