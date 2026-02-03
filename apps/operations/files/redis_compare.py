#!/usr/bin/env python3

"""
redis_compare.py - Compare Redis databases via digest or diff modes

Dependencies:
-------------
pip install redis pyyaml

YAML Config Example:
--------------------
# Connection A (required for diff mode and digest --target a/both)
a:
  mode: url
  url: redis://localhost:6379/0
  password: null
  socket_timeout: 3.0
  decode_responses: true

# Connection B (required for diff mode and digest --target b/both)
b:
  mode: sentinel
  sentinels:
    - host: your-host-here
      port: 26379
  master_name: cluster_name
  password: yourpass
  db: 0
  socket_timeout: 3.0
  decode_responses: true

# Options (all optional with defaults shown)
options:
  # scan_count: Hint for Redis SCAN command - how many keys to return per SCAN iteration.
  #   Affects: Key enumeration memory usage and SCAN call frequency.
  #   Higher = fewer SCAN calls, more memory per iteration.
  #   Default is usually sufficient. Minimal impact on latency.
  scan_count: 1000
  
  # pipeline_batch: Number of Redis commands batched into single pipeline execution.
  #   Affects: Network round-trips for TYPE/GET/HGETALL/LRANGE/SMEMBERS/ZRANGE commands.
  #   Higher = fewer network round-trips, faster performance on high-latency connections.
  #   CRITICAL FOR HIGH LATENCY: Increase to 5000-10000 for slow networks.
  #   Example: 10k keys with batch=1000 → 20 round-trips vs batch=5000 → 4 round-trips
  #   Progress is reported after each batch completes.
  pipeline_batch: 1000
  
  # allow_unsupported_types: Skip keys with unsupported Redis types (e.g., streams, modules).
  allow_unsupported_types: false
  
  # check_values: In diff mode, compare actual values (not just key presence).
  #   Increases network usage and comparison time.
  check_values: false
  
  # check_ttl: In diff mode, compare TTL values between databases.
  check_ttl: false
  
  # ttl_tolerance: Acceptable TTL difference in seconds (when check_ttl: true).
  ttl_tolerance: 120

Usage:
------
# Digest mode - compute digest for both A and B, then compare
python redis_compare.py --config /path/to/config.yaml digest

# Digest mode - compute digest only for A
python redis_compare.py --config /path/to/config.yaml digest --target a

# Digest mode - compute digest only for B
python redis_compare.py --config /path/to/config.yaml digest --target b

# Diff mode - compare keys between A and B
python redis_compare.py --config /path/to/config.yaml diff

Output Format (diff mode):
---------------------------
ONLY_IN_A <key>                    # Key exists only in database A
ONLY_IN_B <key>                    # Key exists only in database B
DIFF_VALUE <key> a=<hex> b=<hex>   # Key value differs (with check_values: true)
DIFF_TTL <key> a=<ttl> b=<ttl>     # Key TTL differs (with check_ttl: true)
                                   # TTL format: "no_expiry" or "NNNs" (seconds)
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from typing import Any, Dict, Iterator, List, Tuple

# Import guards
try:
    import yaml
except Exception as e:
    raise SystemExit(
        "Missing dependency: PyYAML.\n"
        "Install it with: pip install pyyaml\n"
        f"Import error: {e!r}"
    )

try:
    import redis
    from redis.exceptions import RedisError
    from redis.sentinel import Sentinel
except Exception as e:
    raise SystemExit(
        "Missing dependency: redis-py.\n"
        "Install it with: pip install redis\n"
        f"Import error: {e!r}"
    )


DEFAULT_DECODE_RESPONSES = True
# 17 digits preserves IEEE-754 double round-tripping for zset scores.
FLOAT_SCORE_FORMAT = ".17g"


def _die(msg: str, code: int = 2) -> None:
    """Print error message to stderr and exit."""
    sys.stderr.write(msg.rstrip() + "\n")
    raise SystemExit(code)


def load_config(
    path: str,
    require_a: bool = True,
    require_b: bool = True,
) -> Tuple[Dict[str, Any] | None, Dict[str, Any] | None, Dict[str, Any]]:
    """Load YAML config and return (a_conf, b_conf, options)."""
    try:
        with open(path, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        _die(f"Config file not found: {path}", code=2)
    except yaml.YAMLError as e:
        _die(f"Failed to parse YAML config: {e!r}", code=2)
    except Exception as e:
        _die(f"Failed to read config file: {e!r}", code=2)

    if not isinstance(config, dict):
        _die("Config must be a YAML dictionary", code=2)

    a_conf = config.get("a")
    b_conf = config.get("b")
    
    if require_a and a_conf is None:
        _die("Config missing required key: 'a'", code=2)
    
    if require_b and b_conf is None:
        _die("Config missing required key: 'b'", code=2)

    options = config.get("options", {})

    # Set option defaults
    options.setdefault("scan_count", 1000)
    options.setdefault("pipeline_batch", 1000)
    options.setdefault("allow_unsupported_types", False)
    options.setdefault("check_values", False)
    options.setdefault("check_ttl", False)
    options.setdefault("ttl_tolerance", 120)

    return a_conf, b_conf, options


def connect(conf: Dict[str, Any]) -> redis.Redis:
    """
    Connect to Redis using either URL or Sentinel mode.
    Returns a connected Redis client after successful PING.
    """
    mode = conf.get("mode")
    if not mode:
        _die("Connection config missing 'mode' field", code=2)

    if mode == "url":
        url = conf.get("url")
        if not url:
            _die("URL mode requires 'url' field", code=2)
        
        password = conf.get("password")
        socket_timeout = conf.get("socket_timeout", 3.0)
        decode_responses = conf.get("decode_responses", DEFAULT_DECODE_RESPONSES)

        try:
            connection_kwargs = {
                "password": password,
                "socket_timeout": socket_timeout,
                "decode_responses": decode_responses,
            }
            # Only add ssl_cert_reqs if using SSL (rediss://)
            if url.startswith("rediss://"):
                connection_kwargs["ssl_cert_reqs"] = None
            
            r = redis.Redis.from_url(url, **connection_kwargs)
        except Exception as e:
            _die(f"Failed to create Redis client from URL: {e!r}", code=3)

    elif mode == "sentinel":
        sentinels_conf = conf.get("sentinels")
        if not sentinels_conf:
            _die("Sentinel mode requires 'sentinels' field", code=2)

        master_name = conf.get("master_name")
        if not master_name:
            _die("Sentinel mode requires 'master_name' field", code=2)

        password = conf.get("password")
        db = conf.get("db", 0)
        socket_timeout = conf.get("socket_timeout", 3.0)
        decode_responses = conf.get("decode_responses", DEFAULT_DECODE_RESPONSES)

        if password in (None, ""):
            sys.stderr.write(
                "Warning: Sentinel password is empty or unset. "
                "Verify this is intended for your environment.\n"
            )
            sys.stderr.flush()

        # Parse sentinels list
        sentinels = []
        for s in sentinels_conf:
            if not isinstance(s, dict):
                _die(f"Sentinel entry must be a dict with 'host' and 'port': {s!r}", code=2)
            host = s.get("host")
            port = s.get("port", 26379)
            if not host:
                _die(f"Sentinel entry missing 'host': {s!r}", code=2)
            sentinels.append((host, port))

        try:
            # Check if SSL is configured
            use_ssl = conf.get("ssl", False) or conf.get("use_ssl", False)
            
            sentinel_kwargs = {
                "password": password,
                "socket_timeout": socket_timeout,
                "decode_responses": decode_responses,
            }
            
            sentinel_init_kwargs = {
                "socket_timeout": socket_timeout,
                "decode_responses": decode_responses,
            }
            
            master_kwargs = {
                "password": password,
                "db": db,
                "socket_timeout": socket_timeout,
                "decode_responses": decode_responses,
            }
            
            # Only add SSL params if SSL is enabled
            if use_ssl:
                sentinel_kwargs["ssl_cert_reqs"] = None
                sentinel_init_kwargs["ssl_cert_reqs"] = None
                master_kwargs["ssl_cert_reqs"] = None
            
            sentinel = Sentinel(
                sentinels,
                **sentinel_init_kwargs,
                sentinel_kwargs=sentinel_kwargs,
            )
            r = sentinel.master_for(master_name, **master_kwargs)
        except Exception as e:
            _die(f"Failed to create Redis client via Sentinel: {e!r}", code=3)

    else:
        _die(f"Unknown connection mode: {mode!r} (must be 'url' or 'sentinel')", code=2)

    # Validate connection with PING
    try:
        r.ping()
    except RedisError as e:
        _die(f"Failed to PING Redis: {e!r}", code=3)

    return r


def iter_keys(r: redis.Redis, scan_count: int) -> Iterator[str]:
    """Iterate all keys in the database using SCAN."""
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, count=scan_count)
        for key in keys:
            yield key
        if cursor == 0:
            break


def _canonical_value_bytes_from_data(val: Any, typ: str) -> bytes:
    """
    Convert already-fetched Redis data to canonical byte representation.
    Used for pipelined batch operations.
    """
    if typ == "string":
        if val is None:
            return b""
        if isinstance(val, str):
            return val.encode("utf-8")
        if isinstance(val, (bytes, bytearray, memoryview)):
            return bytes(val)
        return bytes(val)

    elif typ == "hash":
        if not val:
            return b""
        items = sorted(val.items())
        parts = []
        for field, value in items:
            if isinstance(field, str):
                field = field.encode("utf-8")
            if isinstance(value, str):
                value = value.encode("utf-8")
            parts.append(field + b"\0" + value)
        return b"\0".join(parts)

    elif typ == "list":
        if not val:
            return b""
        parts = []
        for item in val:
            if isinstance(item, str):
                item = item.encode("utf-8")
            parts.append(item)
        return b"\0".join(parts)

    elif typ == "set":
        if not val:
            return b""
        members = sorted(val)
        parts = []
        for member in members:
            if isinstance(member, str):
                member = member.encode("utf-8")
            parts.append(member)
        return b"\0".join(parts)

    elif typ == "zset":
        if not val:
            return b""
        parts = []
        for member, score in val:
            if isinstance(member, str):
                member = member.encode("utf-8")
            score_str = f"{score:{FLOAT_SCORE_FORMAT}}".encode("utf-8")
            parts.append(member + b"\0" + score_str)
        return b"\0".join(parts)

    else:
        raise ValueError(f"Unsupported Redis type: {typ!r}")


def canonical_value_bytes(r: redis.Redis, key: str, typ: str, allow_unsupported: bool) -> bytes:
    """
    Return a canonical byte representation of the Redis value.
    Raises ValueError for unsupported types unless allow_unsupported is True.
    """
    if typ == "string":
        val = r.get(key)
    elif typ == "hash":
        val = r.hgetall(key)
    elif typ == "list":
        val = r.lrange(key, 0, -1)
    elif typ == "set":
        val = r.smembers(key)
    elif typ == "zset":
        val = r.zrange(key, 0, -1, withscores=True)
    else:
        if allow_unsupported:
            return b""
        raise ValueError(f"Unsupported Redis type: {typ!r}")
    
    return _canonical_value_bytes_from_data(val, typ)


def digest_db(r: redis.Redis, options: Dict[str, Any], label: str = "") -> Tuple[bytes, Dict[str, Any]]:
    """
    Compute order-independent digest of the Redis database.
    Returns (digest_bytes, stats_dict).
    
    Args:
        r: Redis connection
        options: Options dictionary
        label: Optional label for progress reporting (e.g., "A", "B")
    """
    scan_count = options["scan_count"]
    pipeline_batch = options["pipeline_batch"]
    allow_unsupported = options["allow_unsupported_types"]

    accumulator = bytearray(32)  # 32 bytes for SHA256
    key_count = 0
    total_bytes = 0
    unsupported_count = 0
    type_counts: Dict[str, int] = {}

    try:
        # Stream keys and process in batches to minimize memory usage
        batch_keys = []
        for key in iter_keys(r, scan_count):
            batch_keys.append(key)
            
            # Process batch when it reaches pipeline_batch size
            if len(batch_keys) >= pipeline_batch:
                key_count, total_bytes, unsupported_count = _process_digest_batch(
                    r, batch_keys, accumulator, type_counts, allow_unsupported,
                    key_count, total_bytes, unsupported_count, label
                )
                batch_keys = []  # Clear batch
        
        # Process remaining keys
        if batch_keys:
            key_count, total_bytes, unsupported_count = _process_digest_batch(
                r, batch_keys, accumulator, type_counts, allow_unsupported,
                key_count, total_bytes, unsupported_count, label
            )
        
    except RedisError as e:
        _die(f"Redis error during digest scan: {e!r}", code=4)

    stats = {
        "keys": key_count,
        "bytes": total_bytes,
        "unsupported": unsupported_count,
        "type_counts": type_counts,
    }

    return bytes(accumulator), stats


def _process_digest_batch(
    r: redis.Redis,
    batch_keys: List[str],
    accumulator: bytearray,
    type_counts: Dict[str, int],
    allow_unsupported: bool,
    key_count: int,
    total_bytes: int,
    unsupported_count: int,
    label: str,
) -> Tuple[int, int, int]:
    """
    Process a batch of keys for digest computation.
    Returns (updated_key_count, updated_total_bytes, updated_unsupported_count).
    """
    if not batch_keys:
        return key_count, total_bytes, unsupported_count
    
    # Pipeline TYPE commands
    pipe = r.pipeline(transaction=False)
    for key in batch_keys:
        pipe.type(key)
    types = pipe.execute()
    
    # Group keys by type for batch value retrieval
    keys_by_type: Dict[str, List[str]] = {}
    for key, typ in zip(batch_keys, types):
        if typ not in keys_by_type:
            keys_by_type[typ] = []
        keys_by_type[typ].append(key)
        type_counts[typ] = type_counts.get(typ, 0) + 1
    
    # Fetch values by type using pipeline
    values_map: Dict[str, bytes] = {}
    
    for typ, keys in keys_by_type.items():
        pipe = r.pipeline(transaction=False)
        
        for key in keys:
            if typ == "string":
                pipe.get(key)
            elif typ == "hash":
                pipe.hgetall(key)
            elif typ == "list":
                pipe.lrange(key, 0, -1)
            elif typ == "set":
                pipe.smembers(key)
            elif typ == "zset":
                pipe.zrange(key, 0, -1, withscores=True)
            else:
                # Unsupported type
                if allow_unsupported:
                    values_map[key] = b""
                else:
                    unsupported_count += 1
        
        if typ in ["string", "hash", "list", "set", "zset"]:
            results = pipe.execute()
            
            # Convert results to canonical bytes
            for key, val in zip(keys, results):
                try:
                    values_map[key] = _canonical_value_bytes_from_data(val, typ)
                except ValueError:
                    unsupported_count += 1
                    if not allow_unsupported:
                        _die(
                            f"Encountered unsupported type '{typ}' for key '{key}'. "
                            f"Set options.allow_unsupported_types=true to skip.\n"
                            f"Type counts so far: {type_counts}",
                            code=4,
                        )
                    values_map[key] = b""
    
    # Process each key in batch
    for key, typ in zip(batch_keys, types):
        key_count += 1
        
        if key not in values_map:
            if not allow_unsupported:
                _die(
                    f"Encountered unsupported type '{typ}' for key '{key}'. "
                    f"Set options.allow_unsupported_types=true to skip.\n"
                    f"Type counts so far: {type_counts}",
                    code=4,
                )
            continue
        
        val_bytes = values_map[key]

        # Build canonical representation: key + "\0" + type + "\0" + value
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        typ_bytes = typ.encode("utf-8") if isinstance(typ, str) else typ
        canonical = key_bytes + b"\0" + typ_bytes + b"\0" + val_bytes

        # Hash and XOR into accumulator
        h = hashlib.sha256(canonical).digest()
        for i in range(32):
            accumulator[i] ^= h[i]

        total_bytes += len(canonical)
    
    # Progress reporting after each batch
    if label:
        sys.stderr.write(f"[{label}] Processed {key_count} keys, {total_bytes} bytes\n")
        sys.stderr.flush()
    
    return key_count, total_bytes, unsupported_count


def _check_existence_batch(
    target: redis.Redis,
    keys: List[str],
) -> Tuple[List[str], List[str]]:
    """
    Check which keys exist in the target database.
    Returns (keys_not_in_target, keys_in_target).
    """
    pipe = target.pipeline(transaction=False)
    for key in keys:
        pipe.exists(key)
    results = pipe.execute()
    
    keys_not_in_target = []
    keys_in_target = []
    for key, exists in zip(keys, results):
        if exists:
            keys_in_target.append(key)
        else:
            keys_not_in_target.append(key)
    
    return keys_not_in_target, keys_in_target


def _check_values_ttl_batch(
    a: redis.Redis,
    b: redis.Redis,
    keys: List[str],
    check_values: bool,
    check_ttl: bool,
    ttl_tolerance: int,
    allow_unsupported: bool,
) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, int, int]]]:
    """
    Check value and TTL differences for keys that exist in both databases.
    Returns (diff_values, diff_ttls).
    """
    diff_values = []
    diff_ttls = []
    
    if not keys:
        return diff_values, diff_ttls
    
    # Check values
    if check_values:
        # Pipeline TYPE commands for both databases
        pipe_a = a.pipeline(transaction=False)
        pipe_b = b.pipeline(transaction=False)
        for key in keys:
            pipe_a.type(key)
            pipe_b.type(key)
        types_a = pipe_a.execute()
        types_b = pipe_b.execute()
        
        # Group keys by type for batch value retrieval from database A
        keys_by_type_a: Dict[str, List[Tuple[str, str]]] = {}  # type -> [(key, typ_b), ...]
        for key, typ_a, typ_b in zip(keys, types_a, types_b):
            if typ_a != typ_b:
                # Store type mismatch info
                diff_values.append((key, f"type:{typ_a}", f"type:{typ_b}"))
            else:
                if typ_a not in keys_by_type_a:
                    keys_by_type_a[typ_a] = []
                keys_by_type_a[typ_a].append((key, typ_b))
        
        # Fetch values from database A using pipeline
        values_a: Dict[str, bytes] = {}
        for typ, key_pairs in keys_by_type_a.items():
            typ_keys = [k for k, _ in key_pairs]
            pipe = a.pipeline(transaction=False)
            
            for key in typ_keys:
                if typ == "string":
                    pipe.get(key)
                elif typ == "hash":
                    pipe.hgetall(key)
                elif typ == "list":
                    pipe.lrange(key, 0, -1)
                elif typ == "set":
                    pipe.smembers(key)
                elif typ == "zset":
                    pipe.zrange(key, 0, -1, withscores=True)
                else:
                    # Unsupported type
                    if allow_unsupported:
                        values_a[key] = b""
            
            if typ in ["string", "hash", "list", "set", "zset"]:
                results = pipe.execute()
                
                for key, val in zip(typ_keys, results):
                    try:
                        values_a[key] = _canonical_value_bytes_from_data(val, typ)
                    except ValueError:
                        if not allow_unsupported:
                            _die(
                                f"Encountered unsupported type '{typ}' for key '{key}' during value check. "
                                f"Set options.allow_unsupported_types=true to skip.",
                                code=4,
                            )
                        values_a[key] = b""
        
        # Fetch values from database B using pipeline
        values_b: Dict[str, bytes] = {}
        for typ, key_pairs in keys_by_type_a.items():
            typ_keys = [k for k, _ in key_pairs]
            pipe = b.pipeline(transaction=False)
            
            for key in typ_keys:
                if typ == "string":
                    pipe.get(key)
                elif typ == "hash":
                    pipe.hgetall(key)
                elif typ == "list":
                    pipe.lrange(key, 0, -1)
                elif typ == "set":
                    pipe.smembers(key)
                elif typ == "zset":
                    pipe.zrange(key, 0, -1, withscores=True)
                else:
                    # Unsupported type
                    if allow_unsupported:
                        values_b[key] = b""
            
            if typ in ["string", "hash", "list", "set", "zset"]:
                results = pipe.execute()
                
                for key, val in zip(typ_keys, results):
                    try:
                        values_b[key] = _canonical_value_bytes_from_data(val, typ)
                    except ValueError:
                        if not allow_unsupported:
                            _die(
                                f"Encountered unsupported type '{typ}' for key '{key}' during value check. "
                                f"Set options.allow_unsupported_types=true to skip.",
                                code=4,
                            )
                        values_b[key] = b""
        
        # Compare values
        for key in values_a:
            if key in values_b:
                val_a = values_a[key]
                val_b = values_b[key]
                
                if val_a != val_b:
                    # Store actual values (hex for binary safety)
                    diff_values.append((key, val_a.hex(), val_b.hex()))
    
    # Check TTL
    if check_ttl:
        # Pipeline TTL commands for both databases
        pipe_a = a.pipeline(transaction=False)
        pipe_b = b.pipeline(transaction=False)
        for key in keys:
            pipe_a.ttl(key)
            pipe_b.ttl(key)
        ttls_a = pipe_a.execute()
        ttls_b = pipe_b.execute()
        
        # Compare TTLs
        for key, ttl_a, ttl_b in zip(keys, ttls_a, ttls_b):
            # TTL returns -1 for keys with no expiry, -2 for keys that don't exist
            # Compare TTLs based on their states
            if ttl_a == -1 and ttl_b == -1:
                # Both have no expiry - match
                continue
            elif ttl_a == -1 or ttl_b == -1:
                # One has expiry, one doesn't - mismatch
                diff_ttls.append((key, ttl_a, ttl_b))
            elif ttl_a >= 0 and ttl_b >= 0:
                # Both have expiry - check tolerance
                if abs(ttl_a - ttl_b) > ttl_tolerance:
                    diff_ttls.append((key, ttl_a, ttl_b))
            # If either is -2 (doesn't exist), skip (shouldn't happen as we checked existence)
    
    return diff_values, diff_ttls


def diff_presence(
    a: redis.Redis,
    b: redis.Redis,
    options: Dict[str, Any],
) -> None:
    """
    Compare keys between A and B.
    Print ONLY_IN_A and ONLY_IN_B lines.
    If options.check_values is True, also print DIFF_VALUE with hex-encoded values from both databases.
    If options.check_ttl is True, also print DIFF_TTL with TTL values from both databases.
    
    Memory-optimized: processes keys in batches to use at most ~(pipeline_batch*2) memory.
    """
    scan_count = options["scan_count"]
    pipeline_batch = options["pipeline_batch"]
    check_values = options["check_values"]
    check_ttl = options["check_ttl"]
    ttl_tolerance = options["ttl_tolerance"]
    allow_unsupported = options["allow_unsupported_types"]

    only_in_a = []
    only_in_b = []
    diff_values = []  # List of (key, val_a, val_b)
    diff_ttls = []  # List of (key, ttl_a, ttl_b)

    try:
        # Pass A -> B: stream keys from A, check existence in B, and optionally check values/TTL
        sys.stderr.write("Scanning keys in database A...\n")
        sys.stderr.flush()
        
        keys_a_count = 0
        keys_in_both_count = 0
        batch = []
        
        for key in iter_keys(a, scan_count):
            batch.append(key)
            keys_a_count += 1
            
            # Process batch when it reaches pipeline_batch size
            if len(batch) >= pipeline_batch:
                batch_only_in_a, batch_keys_in_both = _check_existence_batch(b, batch)
                only_in_a.extend(batch_only_in_a)
                keys_in_both_count += len(batch_keys_in_both)
                
                # Check values and TTL for keys in both
                if check_values or check_ttl:
                    batch_diff_values, batch_diff_ttls = _check_values_ttl_batch(
                        a, b, batch_keys_in_both, check_values, check_ttl,
                        ttl_tolerance, allow_unsupported
                    )
                    diff_values.extend(batch_diff_values)
                    diff_ttls.extend(batch_diff_ttls)
                
                # Progress reporting
                sys.stderr.write(f"[A→B] Processed {keys_a_count} keys from A\n")
                sys.stderr.flush()
                
                batch = []  # Clear batch
        
        # Process remaining keys
        if batch:
            batch_only_in_a, batch_keys_in_both = _check_existence_batch(b, batch)
            only_in_a.extend(batch_only_in_a)
            keys_in_both_count += len(batch_keys_in_both)
            
            if check_values or check_ttl:
                batch_diff_values, batch_diff_ttls = _check_values_ttl_batch(
                    a, b, batch_keys_in_both, check_values, check_ttl,
                    ttl_tolerance, allow_unsupported
                )
                diff_values.extend(batch_diff_values)
                diff_ttls.extend(batch_diff_ttls)
            
            sys.stderr.write(f"[A→B] Processed {keys_a_count} keys from A\n")
            sys.stderr.flush()
        
        sys.stderr.write(f"[A] Total keys: {keys_a_count}\n")
        sys.stderr.write(f"[A→B] Completed existence check\n")
        if check_values or check_ttl:
            sys.stderr.write(f"Keys in both databases: {keys_in_both_count}\n")
        sys.stderr.flush()
        # Pass B -> A: stream keys from B and check existence in A
        sys.stderr.write("Scanning keys in database B...\n")
        sys.stderr.flush()
        
        keys_b_count = 0
        batch = []
        
        for key in iter_keys(b, scan_count):
            batch.append(key)
            keys_b_count += 1
            
            # Process batch when it reaches pipeline_batch size
            if len(batch) >= pipeline_batch:
                batch_only_in_b, _ = _check_existence_batch(a, batch)
                only_in_b.extend(batch_only_in_b)
                
                # Progress reporting
                sys.stderr.write(f"[B→A] Processed {keys_b_count} keys from B\n")
                sys.stderr.flush()
                
                batch = []  # Clear batch
        
        # Process remaining keys
        if batch:
            batch_only_in_b, _ = _check_existence_batch(a, batch)
            only_in_b.extend(batch_only_in_b)
            
            sys.stderr.write(f"[B→A] Processed {keys_b_count} keys from B\n")
            sys.stderr.flush()
        
        sys.stderr.write(f"[B] Total keys: {keys_b_count}\n")
        sys.stderr.write(f"[B→A] Completed existence check\n")
        sys.stderr.write("Comparison complete.\n")
        sys.stderr.flush()

    except RedisError as e:
        _die(f"Redis error during diff: {e!r}", code=4)

    # Print results in table format
    has_output = False
    
    if only_in_a:
        has_output = True
        print("\n" + "=" * 80)
        print("KEYS ONLY IN DATABASE A")
        print("=" * 80)
        print(f"{'Key':<78}")
        print("-" * 80)
        for key in only_in_a:
            print(f"{key:<78}")
    
    if only_in_b:
        has_output = True
        print("\n" + "=" * 80)
        print("KEYS ONLY IN DATABASE B")
        print("=" * 80)
        print(f"{'Key':<78}")
        print("-" * 80)
        for key in only_in_b:
            print(f"{key:<78}")
    
    if check_values and diff_values:
        has_output = True
        print("\n" + "=" * 80)
        print("VALUE DIFFERENCES")
        print("=" * 80)
        print(f"{'Key':<30} {'Value in A':<24} {'Value in B':<24}")
        print("-" * 80)
        for key, val_a, val_b in diff_values:
            # Truncate long values for display
            val_a_display = val_a[:22] + ".." if len(val_a) > 24 else val_a
            val_b_display = val_b[:22] + ".." if len(val_b) > 24 else val_b
            print(f"{key:<30} {val_a_display:<24} {val_b_display:<24}")
    
    if check_ttl and diff_ttls:
        has_output = True
        print("\n" + "=" * 80)
        print(f"TTL DIFFERENCES (tolerance: ±{ttl_tolerance}s)")
        print("=" * 80)
        print(f"{'Key':<50} {'TTL in A':<14} {'TTL in B':<14}")
        print("-" * 80)
        for key, ttl_a, ttl_b in diff_ttls:
            # Format TTL values: -1 means no expiry, >= 0 is seconds
            ttl_a_str = "no_expiry" if ttl_a == -1 else f"{ttl_a}s"
            ttl_b_str = "no_expiry" if ttl_b == -1 else f"{ttl_b}s"
            print(f"{key:<50} {ttl_a_str:<14} {ttl_b_str:<14}")
    
    if has_output:
        print("\n" + "=" * 80)


def mode_digest(
    a: redis.Redis | None,
    b: redis.Redis | None,
    options: Dict[str, Any],
    target: str,
) -> int:
    """
    Digest mode: compute digest(s) based on target.
    target can be 'a', 'b', or 'both'.
    """
    if target == "a":
        if a is None:
            _die("Target 'a' specified but connection A not configured", code=2)
        digest_a, stats_a = digest_db(a, options, label="A")
        print(f"A_DIGEST {digest_a.hex()} keys={stats_a['keys']} bytes={stats_a['bytes']}")
        return 0

    elif target == "b":
        if b is None:
            _die("Target 'b' specified but connection B not configured", code=2)
        digest_b, stats_b = digest_db(b, options, label="B")
        print(f"B_DIGEST {digest_b.hex()} keys={stats_b['keys']} bytes={stats_b['bytes']}")
        return 0

    elif target == "both":
        if a is None or b is None:
            _die("Target 'both' requires both A and B connections to be configured", code=2)
        
        sys.stderr.write("Computing digest for database A...\n")
        sys.stderr.flush()
        digest_a, stats_a = digest_db(a, options, label="A")
        
        sys.stderr.write("Computing digest for database B...\n")
        sys.stderr.flush()
        digest_b, stats_b = digest_db(b, options, label="B")

        digest_a_hex = digest_a.hex()
        digest_b_hex = digest_b.hex()

        print(f"A_DIGEST {digest_a_hex} keys={stats_a['keys']} bytes={stats_a['bytes']}")
        print(f"B_DIGEST {digest_b_hex} keys={stats_b['keys']} bytes={stats_b['bytes']}")

        match = digest_a == digest_b
        print(f"DIGEST_MATCH {str(match).lower()}")

        return 0 if match else 1

    else:
        _die(f"Invalid target: {target!r} (must be 'a', 'b', or 'both')", code=2)


def mode_diff(a: redis.Redis, b: redis.Redis, options: Dict[str, Any]) -> int:
    """
    Diff mode: compare keys between A and B, print differences, return 0.
    """
    diff_presence(a, b, options)
    return 0


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Compare Redis databases via digest or diff modes"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to YAML config file",
    )
    parser.add_argument(
        "mode",
        choices=["digest", "diff"],
        help="Comparison mode",
    )
    parser.add_argument(
        "--target",
        choices=["a", "b", "both"],
        default="both",
        help="Target for digest mode: 'a', 'b', or 'both' (default: both)",
    )

    args = parser.parse_args(argv[1:])

    # Determine which connections are required
    require_a = args.mode == "diff" or args.target in ["a", "both"]
    require_b = args.mode == "diff" or args.target in ["b", "both"]
    
    # Load config
    a_conf, b_conf, options = load_config(args.config, require_a=require_a, require_b=require_b)

    # Connect to Redis instances as needed
    a = None
    b = None
    
    if require_a:
        a = connect(a_conf)
    
    if require_b:
        b = connect(b_conf)

    # Run selected mode
    if args.mode == "digest":
        return mode_digest(a, b, options, args.target)
    elif args.mode == "diff":
        return mode_diff(a, b, options)
    else:
        _die(f"Unknown mode: {args.mode!r}", code=2)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))