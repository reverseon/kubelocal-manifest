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
  scan_count: 1000
  pipeline_batch: 1000
  allow_unsupported_types: false
  check_values: false
  check_ttl: false
  ttl_tolerance: 120  # Seconds of acceptable TTL difference
  progress_interval: 1000  # Report progress every N keys

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
BATCH_PROGRESS_MULTIPLIER = 10
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
    options.setdefault("progress_interval", 1000)

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


def canonical_value_bytes(r: redis.Redis, key: str, typ: str, allow_unsupported: bool) -> bytes:
    """
    Return a canonical byte representation of the Redis value.
    Raises ValueError for unsupported types unless allow_unsupported is True.
    """
    if typ == "string":
        val = r.get(key)
        if val is None:
            return b""
        # redis-py with decode_responses=True returns str; False returns bytes.
        if isinstance(val, str):
            return val.encode("utf-8")
        if isinstance(val, (bytes, bytearray, memoryview)):
            return bytes(val)
        return bytes(val)

    elif typ == "hash":
        # HGETALL returns dict
        val = r.hgetall(key)
        if not val:
            return b""
        # Sort by field name for determinism
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
        val = r.lrange(key, 0, -1)
        if not val:
            return b""
        parts = []
        for item in val:
            if isinstance(item, str):
                item = item.encode("utf-8")
            parts.append(item)
        return b"\0".join(parts)

    elif typ == "set":
        val = r.smembers(key)
        if not val:
            return b""
        # Sort members for determinism
        members = sorted(val)
        parts = []
        for member in members:
            if isinstance(member, str):
                member = member.encode("utf-8")
            parts.append(member)
        return b"\0".join(parts)

    elif typ == "zset":
        # ZRANGE with WITHSCORES returns list of (member, score) tuples
        val = r.zrange(key, 0, -1, withscores=True)
        if not val:
            return b""
        parts = []
        for member, score in val:
            if isinstance(member, str):
                member = member.encode("utf-8")
            # Normalize score representation
            score_str = f"{score:{FLOAT_SCORE_FORMAT}}".encode("utf-8")
            parts.append(member + b"\0" + score_str)
        return b"\0".join(parts)

    else:
        if allow_unsupported:
            return b""
        raise ValueError(f"Unsupported Redis type: {typ!r}")


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
    allow_unsupported = options["allow_unsupported_types"]
    progress_interval = options.get("progress_interval", 1000)

    accumulator = bytearray(32)  # 32 bytes for SHA256
    key_count = 0
    total_bytes = 0
    unsupported_count = 0
    type_counts: Dict[str, int] = {}

    try:
        for key in iter_keys(r, scan_count):
            key_count += 1
            typ = r.type(key)
            type_counts[typ] = type_counts.get(typ, 0) + 1

            try:
                val_bytes = canonical_value_bytes(r, key, typ, allow_unsupported)
            except ValueError:
                unsupported_count += 1
                if not allow_unsupported:
                    _die(
                        f"Encountered unsupported type '{typ}' for key '{key}'. "
                        f"Set options.allow_unsupported_types=true to skip.\n"
                        f"Type counts so far: {type_counts}",
                        code=4,
                    )
                continue

            # Build canonical representation: key + "\0" + type + "\0" + value
            key_bytes = key.encode("utf-8") if isinstance(key, str) else key
            typ_bytes = typ.encode("utf-8") if isinstance(typ, str) else typ
            canonical = key_bytes + b"\0" + typ_bytes + b"\0" + val_bytes

            # Hash and XOR into accumulator
            h = hashlib.sha256(canonical).digest()
            for i in range(32):
                accumulator[i] ^= h[i]

            total_bytes += len(canonical)
            
            # Progress reporting
            if label and key_count % progress_interval == 0:
                sys.stderr.write(f"[{label}] Processed {key_count} keys, {total_bytes} bytes\n")
                sys.stderr.flush()

    except RedisError as e:
        _die(f"Redis error during digest scan: {e!r}", code=4)

    stats = {
        "keys": key_count,
        "bytes": total_bytes,
        "unsupported": unsupported_count,
        "type_counts": type_counts,
    }

    return bytes(accumulator), stats


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
    """
    scan_count = options["scan_count"]
    pipeline_batch = options["pipeline_batch"]
    check_values = options["check_values"]
    check_ttl = options["check_ttl"]
    ttl_tolerance = options["ttl_tolerance"]
    allow_unsupported = options["allow_unsupported_types"]
    progress_interval = options.get("progress_interval", 1000)

    only_in_a = []
    only_in_b = []
    diff_values = []  # List of (key, val_a, val_b)
    diff_ttls = []  # List of (key, ttl_a, ttl_b)

    try:
        # Pass A -> B: find keys in A not in B
        sys.stderr.write("Scanning keys in database A...\n")
        sys.stderr.flush()
        keys_a = []
        scanned_a = 0
        for key in iter_keys(a, scan_count):
            keys_a.append(key)
            scanned_a += 1
            if scanned_a % progress_interval == 0:
                sys.stderr.write(f"[A] Scanned {scanned_a} keys\n")
                sys.stderr.flush()
        
        sys.stderr.write(f"[A] Total keys: {len(keys_a)}\n")
        sys.stderr.write("Checking existence in database B...\n")
        sys.stderr.flush()
        
        # Batch EXISTS checks
        for i in range(0, len(keys_a), pipeline_batch):
            if i > 0 and i % (progress_interval * BATCH_PROGRESS_MULTIPLIER) == 0:
                sys.stderr.write(f"[A→B] Checked {i}/{len(keys_a)} keys for existence\n")
                sys.stderr.flush()
            
            batch = keys_a[i : i + pipeline_batch]
            pipe = b.pipeline(transaction=False)
            for key in batch:
                pipe.exists(key)
            results = pipe.execute()
            
            for key, exists in zip(batch, results):
                if not exists:
                    only_in_a.append(key)
        
        sys.stderr.write(f"[A→B] Completed existence check for {len(keys_a)} keys\n")
        sys.stderr.flush()

        # If check_values or check_ttl, we need keys_in_both
        if check_values or check_ttl:
            keys_in_both = [k for k in keys_a if k not in only_in_a]
            sys.stderr.write(f"Keys in both databases: {len(keys_in_both)}\n")
            sys.stderr.flush()
            
            # Check values
            if check_values:
                sys.stderr.write("Checking value differences...\n")
                sys.stderr.flush()
                checked_values = 0
                for key in keys_in_both:
                    checked_values += 1
                    if checked_values % progress_interval == 0:
                        sys.stderr.write(f"[VALUES] Checked {checked_values}/{len(keys_in_both)} keys\n")
                        sys.stderr.flush()
                    typ_a = a.type(key)
                    typ_b = b.type(key)
                    
                    if typ_a != typ_b:
                        # Store type mismatch info
                        diff_values.append((key, f"type:{typ_a}", f"type:{typ_b}"))
                        continue
                    
                    try:
                        val_a = canonical_value_bytes(a, key, typ_a, allow_unsupported)
                        val_b = canonical_value_bytes(b, key, typ_b, allow_unsupported)
                        
                        if val_a != val_b:
                            # Store actual values (hex for binary safety)
                            diff_values.append((key, val_a.hex(), val_b.hex()))
                    except ValueError:
                        # Unsupported type
                        if not allow_unsupported:
                            _die(
                                f"Encountered unsupported type '{typ_a}' for key '{key}' during value check. "
                                f"Set options.allow_unsupported_types=true to skip.",
                                code=4,
                            )
                
                sys.stderr.write(f"[VALUES] Completed checking {len(keys_in_both)} keys\n")
                sys.stderr.flush()
            
            # Check TTL
            if check_ttl:
                sys.stderr.write("Checking TTL differences...\n")
                sys.stderr.flush()
                checked_ttl = 0
                for key in keys_in_both:
                    checked_ttl += 1
                    if checked_ttl % progress_interval == 0:
                        sys.stderr.write(f"[TTL] Checked {checked_ttl}/{len(keys_in_both)} keys\n")
                        sys.stderr.flush()
                    ttl_a = a.ttl(key)
                    ttl_b = b.ttl(key)
                    
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
                
                sys.stderr.write(f"[TTL] Completed checking {len(keys_in_both)} keys\n")
                sys.stderr.flush()

        # Pass B -> A: find keys in B not in A
        sys.stderr.write("Scanning keys in database B...\n")
        sys.stderr.flush()
        keys_b = []
        scanned_b = 0
        for key in iter_keys(b, scan_count):
            keys_b.append(key)
            scanned_b += 1
            if scanned_b % progress_interval == 0:
                sys.stderr.write(f"[B] Scanned {scanned_b} keys\n")
                sys.stderr.flush()
        
        sys.stderr.write(f"[B] Total keys: {len(keys_b)}\n")
        sys.stderr.write("Checking existence in database A...\n")
        sys.stderr.flush()
        
        for i in range(0, len(keys_b), pipeline_batch):
            if i > 0 and i % (progress_interval * BATCH_PROGRESS_MULTIPLIER) == 0:
                sys.stderr.write(f"[B→A] Checked {i}/{len(keys_b)} keys for existence\n")
                sys.stderr.flush()
            
            batch = keys_b[i : i + pipeline_batch]
            pipe = a.pipeline(transaction=False)
            for key in batch:
                pipe.exists(key)
            results = pipe.execute()
            
            for key, exists in zip(batch, results):
                if not exists:
                    only_in_b.append(key)
        
        sys.stderr.write(f"[B→A] Completed existence check for {len(keys_b)} keys\n")
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