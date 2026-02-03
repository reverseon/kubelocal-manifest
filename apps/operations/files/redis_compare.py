#!/usr/bin/env python3

"""
redis_compare.py - Compare Redis databases via digest or diff modes

OVERVIEW:
---------
Tool for comparing Redis databases with two operating modes:

1. DIGEST MODE: Fast order-independent hash comparison
   - Computes SHA256-based digest representing entire database
   - Algorithm: XOR all individual key hashes into single accumulator
   - Use case: Quick check if databases are identical
   - Time complexity: O(n) where n = number of keys

2. DIFF MODE: Detailed key-by-key comparison
   - Two-pass algorithm to find unique and differing keys
   - Pass 1 (A→B): Find keys only in A, compare values/TTLs for common keys
   - Pass 2 (B→A): Find keys only in B
   - Use case: Identify specific differences between databases
   - Time complexity: O(n) but slower due to value comparisons

KEY ALGORITHM FEATURES:
-----------------------
- Memory-efficient: Batch processing with configurable batch sizes
- Network-optimized: Redis pipelining reduces round-trips (critical for high latency)
- Order-independent: Digest uses XOR accumulation (commutative/associative)
- Canonical representation: Deterministic encoding for each Redis type
  * Hashes/Sets: Alphabetically sorted for order independence
  * Zsets: Already ordered by Redis (score ascending)
  * Lists: Order preserved (index matters)

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


# decode_responses=True returns strings instead of bytes for Redis responses,
# making comparison and debugging easier for text-based data
DEFAULT_DECODE_RESPONSES = True

# 17 digits preserves IEEE-754 double round-tripping for zset scores.
# This ensures that floating-point scores in sorted sets can be compared
# accurately without precision loss during string conversion
FLOAT_SCORE_FORMAT = ".17g"


def _die(msg: str, code: int = 2) -> None:
    """
    Print error message to stderr and exit with specified code.
    
    Exit codes:
    - 2: Configuration/argument error
    - 3: Connection error
    - 4: Redis operation error
    
    Args:
        msg: Error message to display
        code: Exit code (default 2)
    """
    sys.stderr.write(msg.rstrip() + "\n")
    raise SystemExit(code)


def load_config(
    path: str,
    require_a: bool = True,
    require_b: bool = True,
) -> Tuple[Dict[str, Any] | None, Dict[str, Any] | None, Dict[str, Any]]:
    """
    Load YAML config and return (a_conf, b_conf, options).
    
    Args:
        path: Filesystem path to YAML configuration file
        require_a: Whether connection A configuration is mandatory (False for digest --target b)
        require_b: Whether connection B configuration is mandatory (False for digest --target a)
    
    Returns:
        Tuple of (a_connection_config, b_connection_config, options_dict)
    """
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
    
    URL mode: Direct connection via redis://host:port/db or rediss://host:port/db (SSL)
    Sentinel mode: High-availability connection via Redis Sentinel cluster for master discovery
    
    Returns a connected Redis client after successful PING validation.
    
    Args:
        conf: Connection configuration dictionary containing 'mode' and mode-specific parameters
    """
    # mode determines connection strategy: 'url' for direct, 'sentinel' for HA cluster
    mode = conf.get("mode")
    if not mode:
        _die("Connection config missing 'mode' field", code=2)

    if mode == "url":
        # URL format: redis://host:port/db or rediss://host:port/db (SSL)
        url = conf.get("url")
        if not url:
            _die("URL mode requires 'url' field", code=2)
        
        # password: Redis AUTH password (can be None for no auth)
        password = conf.get("password")
        # socket_timeout: Maximum seconds to wait for Redis operations before timeout
        socket_timeout = conf.get("socket_timeout", 3.0)
        # decode_responses: When True, returns strings instead of bytes from Redis
        decode_responses = conf.get("decode_responses", DEFAULT_DECODE_RESPONSES)

        try:
            # Build connection parameters for Redis client
            connection_kwargs = {
                "password": password,
                "socket_timeout": socket_timeout,
                "decode_responses": decode_responses,
            }
            # ssl_cert_reqs=None disables certificate verification for SSL connections
            # Only needed for rediss:// (SSL) URLs, not plain redis://
            if url.startswith("rediss://"):
                connection_kwargs["ssl_cert_reqs"] = None
            
            # Create Redis client from URL string
            r = redis.Redis.from_url(url, **connection_kwargs)
        except Exception as e:
            _die(f"Failed to create Redis client from URL: {e!r}", code=3)

    elif mode == "sentinel":
        # Sentinel mode: High-availability Redis setup where Sentinel nodes monitor masters
        # and provide automatic failover. Client queries Sentinels to discover current master.
        
        # sentinels_conf: List of Sentinel nodes [{host, port}, ...] for master discovery
        sentinels_conf = conf.get("sentinels")
        if not sentinels_conf:
            _die("Sentinel mode requires 'sentinels' field", code=2)

        # master_name: Logical name of the Redis master being monitored by Sentinels
        master_name = conf.get("master_name")
        if not master_name:
            _die("Sentinel mode requires 'master_name' field", code=2)

        # password: Redis AUTH password for master connection
        password = conf.get("password")
        # db: Redis database number (0-15 typically)
        db = conf.get("db", 0)
        # socket_timeout: Maximum seconds to wait for Redis operations
        socket_timeout = conf.get("socket_timeout", 3.0)
        # decode_responses: When True, returns strings instead of bytes
        decode_responses = conf.get("decode_responses", DEFAULT_DECODE_RESPONSES)

        if password in (None, ""):
            sys.stderr.write(
                "Warning: Sentinel password is empty or unset. "
                "Verify this is intended for your environment.\n"
            )
            sys.stderr.flush()

        # Parse sentinels list from config into (host, port) tuples
        # Multiple sentinels provide redundancy - client tries each until one responds
        sentinels = []
        for s in sentinels_conf:
            if not isinstance(s, dict):
                _die(f"Sentinel entry must be a dict with 'host' and 'port': {s!r}", code=2)
            # host: Hostname or IP address of Sentinel node
            host = s.get("host")
            # port: Sentinel port (default 26379, not Redis port 6379)
            port = s.get("port", 26379)
            if not host:
                _die(f"Sentinel entry missing 'host': {s!r}", code=2)
            sentinels.append((host, port))

        try:
            # use_ssl: Whether to use SSL/TLS for Sentinel and master connections
            use_ssl = conf.get("ssl", False) or conf.get("use_ssl", False)
            
            # sentinel_kwargs: Parameters passed when Sentinel queries master info
            sentinel_kwargs = {
                "password": password,
                "socket_timeout": socket_timeout,
                "decode_responses": decode_responses,
            }
            
            # sentinel_init_kwargs: Parameters for initial Sentinel connection setup
            sentinel_init_kwargs = {
                "socket_timeout": socket_timeout,
                "decode_responses": decode_responses,
            }
            
            # master_kwargs: Parameters for actual master Redis connection after discovery
            master_kwargs = {
                "password": password,
                "db": db,
                "socket_timeout": socket_timeout,
                "decode_responses": decode_responses,
            }
            
            # ssl_cert_reqs=None disables certificate verification for SSL connections
            # Applied to both Sentinel and master connections when SSL is enabled
            if use_ssl:
                sentinel_kwargs["ssl_cert_reqs"] = None
                sentinel_init_kwargs["ssl_cert_reqs"] = None
                master_kwargs["ssl_cert_reqs"] = None
            
            # Create Sentinel client that connects to Sentinel nodes
            sentinel = Sentinel(
                sentinels,
                **sentinel_init_kwargs,
                sentinel_kwargs=sentinel_kwargs,
            )
            # master_for queries Sentinels to discover current master and returns Redis client
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
    """
    Iterate all keys in the database using SCAN.
    
    SCAN is cursor-based iteration that doesn't block Redis like KEYS command.
    Algorithm: Iterate through key space using cursor, yielding keys as they're found.
    
    Args:
        r: Redis connection
        scan_count: Hint to Redis for number of keys to return per SCAN call (actual may vary)
    
    Yields:
        Key names as strings (or bytes if decode_responses=False)
    """
    # cursor tracks position in keyspace iteration. 0 means start (and end when returned)
    cursor = 0
    while True:
        # SCAN returns (next_cursor, keys_list). count is a hint, not guarantee
        cursor, keys = r.scan(cursor=cursor, count=scan_count)
        for key in keys:
            yield key
        # When cursor returns to 0, iteration is complete
        if cursor == 0:
            break


def _canonical_value_bytes_from_data(val: Any, typ: str) -> bytes:
    """
    Convert already-fetched Redis data to canonical byte representation.
    
    Canonical representation ensures order-independent comparison:
    - Hashes: Fields sorted alphabetically, null-byte delimited
    - Sets: Members sorted alphabetically, null-byte delimited
    - Zsets: Members sorted by score (Redis order), then by name, null-byte delimited
    - Lists: Order preserved (list index matters)
    - Strings: Raw bytes
    
    Algorithm: Normalize data structure into deterministic byte sequence that can be hashed
    for digest comparison or direct comparison for value checking.
    
    Args:
        val: Already-fetched Redis value (result of GET/HGETALL/LRANGE/etc)
        typ: Redis type string ('string', 'hash', 'list', 'set', 'zset')
    
    Returns:
        Canonical bytes representation suitable for hashing or comparison
    
    Used for pipelined batch operations where values are already fetched.
    """
    if typ == "string":
        # String type: Store raw bytes, empty if None
        if val is None:
            return b""
        if isinstance(val, str):
            return val.encode("utf-8")
        if isinstance(val, (bytes, bytearray, memoryview)):
            return bytes(val)
        return bytes(val)

    elif typ == "hash":
        # Hash type (field->value map): Sort fields alphabetically for order-independence
        # Format: field1\0value1\0field2\0value2\0...
        # Null byte (\0) used as delimiter since it's not common in Redis keys/values
        if not val:
            return b""
        # Sort by field name to ensure consistent ordering regardless of HGETALL order
        items = sorted(val.items())
        parts = []
        for field, value in items:
            if isinstance(field, str):
                field = field.encode("utf-8")
            if isinstance(value, str):
                value = value.encode("utf-8")
            # Each field-value pair separated by null byte
            parts.append(field + b"\0" + value)
        # Join all pairs with null bytes
        return b"\0".join(parts)

    elif typ == "list":
        # List type: Preserve order since list index matters
        # Format: item1\0item2\0item3\0...
        if not val:
            return b""
        parts = []
        for item in val:
            if isinstance(item, str):
                item = item.encode("utf-8")
            parts.append(item)
        return b"\0".join(parts)

    elif typ == "set":
        # Set type: Sort members alphabetically for order-independence
        # Format: member1\0member2\0member3\0...
        # Sets are unordered in Redis, so we sort for deterministic representation
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
        # Sorted set type (scored members): Order already determined by Redis ZRANGE
        # Format: member1\0score1\0member2\0score2\0...
        # ZRANGE returns members in score order (ascending), which is deterministic
        if not val:
            return b""
        parts = []
        for member, score in val:
            if isinstance(member, str):
                member = member.encode("utf-8")
            # Use 17-digit precision to preserve IEEE-754 double round-tripping
            score_str = f"{score:{FLOAT_SCORE_FORMAT}}".encode("utf-8")
            parts.append(member + b"\0" + score_str)
        return b"\0".join(parts)

    else:
        raise ValueError(f"Unsupported Redis type: {typ!r}")


def canonical_value_bytes(r: redis.Redis, key: str, typ: str, allow_unsupported: bool) -> bytes:
    """
    Return a canonical byte representation of the Redis value.
    
    Non-pipelined version for single-key operations.
    Fetches value from Redis, then converts to canonical bytes.
    
    For batch operations, use _canonical_value_bytes_from_data directly
    with pre-fetched values to benefit from pipelining.
    
    Raises ValueError for unsupported types unless allow_unsupported is True.
    
    Args:
        r: Redis connection
        key: Redis key to fetch
        typ: Redis type of the key
        allow_unsupported: Return empty bytes for unsupported types instead of raising
    
    Returns:
        Canonical bytes representation
    """
    # Fetch value using appropriate Redis command based on type
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
        # Unsupported type (e.g., stream, module types)
        if allow_unsupported:
            return b""
        raise ValueError(f"Unsupported Redis type: {typ!r}")
    
    # Convert fetched value to canonical bytes
    return _canonical_value_bytes_from_data(val, typ)


def digest_db(r: redis.Redis, options: Dict[str, Any], label: str = "") -> Tuple[bytes, Dict[str, Any]]:
    """
    Compute order-independent digest of the Redis database.
    
    Algorithm:
    1. Iterate all keys using SCAN (non-blocking)
    2. For each key, create canonical representation: key + type + value
    3. Hash each canonical representation with SHA256
    4. XOR all hashes into accumulator for order-independent final digest
    
    XOR properties:
    - Commutative: a⊕b = b⊕a (order doesn't matter)
    - Associative: (a⊕b)⊕c = a⊕(b⊕c) (grouping doesn't matter)
    - Makes digest independent of key iteration order
    
    Memory optimization:
    - Process keys in batches to avoid loading entire database into memory
    - Stream keys through SCAN iterator
    
    Returns (digest_bytes, stats_dict).
    
    Args:
        r: Redis connection
        options: Options dictionary with scan_count, pipeline_batch, allow_unsupported_types
        label: Optional label for progress reporting (e.g., "A", "B")
    """
    # scan_count: Hint for SCAN command - keys per iteration
    scan_count = options["scan_count"]
    # pipeline_batch: Number of keys to process in single pipeline batch
    pipeline_batch = options["pipeline_batch"]
    # allow_unsupported: Skip unsupported types instead of failing
    allow_unsupported = options["allow_unsupported_types"]

    # accumulator: XOR accumulator for order-independent digest (32 bytes for SHA256)
    # All key hashes XORed into this, making final digest independent of key order
    accumulator = bytearray(32)  # 32 bytes for SHA256
    # key_count: Total keys processed
    key_count = 0
    # total_bytes: Total bytes in canonical representations (for stats)
    total_bytes = 0
    # unsupported_count: Count of keys with unsupported types
    unsupported_count = 0
    # type_counts: Histogram of Redis types encountered
    type_counts: Dict[str, int] = {}

    try:
        # Stream keys and process in batches to minimize memory usage
        # Algorithm: Collect keys until batch size reached, process batch, repeat
        # This avoids loading all keys into memory at once
        batch_keys = []
        for key in iter_keys(r, scan_count):
            batch_keys.append(key)
            
            # Process batch when it reaches pipeline_batch size
            # Batch processing reduces network round-trips via pipelining
            if len(batch_keys) >= pipeline_batch:
                key_count, total_bytes, unsupported_count = _process_digest_batch(
                    r, batch_keys, accumulator, type_counts, allow_unsupported,
                    key_count, total_bytes, unsupported_count, label
                )
                # Clear batch to free memory before next iteration
                batch_keys = []
        
        # Process remaining keys that didn't fill a complete batch
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
    Process a batch of keys for digest computation using pipelined Redis operations.
    
    Algorithm:
    1. Pipeline TYPE commands for all keys (1 network round-trip)
    2. Group keys by type
    3. For each type, pipeline value fetch commands (1 round-trip per type)
    4. Convert values to canonical bytes
    5. Hash each key+type+value and XOR into accumulator
    
    Pipelining optimization:
    - Without pipeline: N keys = N round-trips for TYPE + N round-trips for value = 2N
    - With pipeline: N keys = 1 round-trip for TYPE + ~5 round-trips for values (max 5 types)
    - Critical for high-latency connections
    
    Returns (updated_key_count, updated_total_bytes, updated_unsupported_count).
    
    Args:
        r: Redis connection
        batch_keys: List of keys to process in this batch
        accumulator: XOR accumulator for digest (modified in-place)
        type_counts: Type histogram (modified in-place)
        allow_unsupported: Skip unsupported types instead of failing
        key_count: Current key count (returned incremented)
        total_bytes: Current total bytes (returned incremented)
        unsupported_count: Current unsupported count (returned incremented)
        label: Progress label for stderr output
    """
    if not batch_keys:
        return key_count, total_bytes, unsupported_count
    
    # Step 1: Pipeline TYPE commands for all keys in batch
    # transaction=False disables MULTI/EXEC wrapper for better performance
    pipe = r.pipeline(transaction=False)
    for key in batch_keys:
        pipe.type(key)
    # Execute all TYPE commands in single network round-trip
    types = pipe.execute()
    
    # Step 2: Group keys by type for optimized value fetching
    # Each type requires different Redis command (GET vs HGETALL vs LRANGE, etc.)
    keys_by_type: Dict[str, List[str]] = {}
    for key, typ in zip(batch_keys, types):
        if typ not in keys_by_type:
            keys_by_type[typ] = []
        keys_by_type[typ].append(key)
        # Update type histogram
        type_counts[typ] = type_counts.get(typ, 0) + 1
    
    # Step 3: Fetch values by type using pipeline
    # values_map: key -> canonical_bytes mapping for all keys in batch
    values_map: Dict[str, bytes] = {}
    
    # For each type group, pipeline appropriate value-fetch commands
    for typ, keys in keys_by_type.items():
        pipe = r.pipeline(transaction=False)
        
        # Add appropriate command to pipeline based on Redis type
        for key in keys:
            if typ == "string":
                # GET returns raw string value
                pipe.get(key)
            elif typ == "hash":
                # HGETALL returns all field-value pairs as dict
                pipe.hgetall(key)
            elif typ == "list":
                # LRANGE 0 -1 returns all list elements
                pipe.lrange(key, 0, -1)
            elif typ == "set":
                # SMEMBERS returns all set members
                pipe.smembers(key)
            elif typ == "zset":
                # ZRANGE 0 -1 withscores returns all members with scores
                pipe.zrange(key, 0, -1, withscores=True)
            else:
                # Unsupported type (e.g., stream, module types)
                if allow_unsupported:
                    values_map[key] = b""
                else:
                    unsupported_count += 1
        
        # Execute pipeline for this type group (1 network round-trip per type)
        if typ in ["string", "hash", "list", "set", "zset"]:
            results = pipe.execute()
            
            # Convert results to canonical bytes representation
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
    
    # Step 4: Process each key - hash and XOR into accumulator
    for key, typ in zip(batch_keys, types):
        key_count += 1
        
        # Skip keys not in values_map (unsupported types)
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

        # Build canonical representation: key\0type\0value
        # This format ensures each key contributes unique hash to accumulator
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        typ_bytes = typ.encode("utf-8") if isinstance(typ, str) else typ
        canonical = key_bytes + b"\0" + typ_bytes + b"\0" + val_bytes

        # Hash canonical representation with SHA256
        h = hashlib.sha256(canonical).digest()
        # XOR hash into accumulator (32 bytes)
        # XOR ensures order independence: hash1 ⊕ hash2 = hash2 ⊕ hash1
        for i in range(32):
            accumulator[i] ^= h[i]

        total_bytes += len(canonical)
    
    # Progress reporting after each batch completes
    if label:
        sys.stderr.write(f"[{label}] Processed {key_count} keys, {total_bytes} bytes\n")
        sys.stderr.flush()
    
    return key_count, total_bytes, unsupported_count


def _check_existence_batch(
    target: redis.Redis,
    keys: List[str],
) -> Tuple[List[str], List[str]]:
    """
    Check which keys exist in the target database using pipelined EXISTS commands.
    
    Algorithm:
    1. Pipeline EXISTS commands for all keys (1 network round-trip)
    2. Partition keys into exists/not-exists lists based on results
    
    Used in diff algorithm to find keys unique to source database.
    
    Returns (keys_not_in_target, keys_in_target).
    
    Args:
        target: Redis connection to check against
        keys: List of keys to check for existence
    """
    # Pipeline EXISTS commands for all keys
    pipe = target.pipeline(transaction=False)
    for key in keys:
        pipe.exists(key)
    # Execute all EXISTS commands in single network round-trip
    # Results are list of integers: 1 if exists, 0 if not
    results = pipe.execute()
    
    # Partition keys based on existence in target
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
    
    Algorithm for value comparison:
    1. Pipeline TYPE commands for both databases (2 round-trips)
    2. Compare types - if different, report type mismatch
    3. Group keys by type and pipeline value fetches (2*5 round-trips max)
    4. Convert to canonical bytes and compare
    
    Algorithm for TTL comparison:
    1. Pipeline TTL commands for both databases (2 round-trips)
    2. Compare TTLs respecting tolerance window
    
    Returns (diff_values, diff_ttls).
    
    Args:
        a: Redis connection A
        b: Redis connection B
        keys: List of keys that exist in both databases
        check_values: Whether to compare values
        check_ttl: Whether to compare TTLs
        ttl_tolerance: Acceptable TTL difference in seconds
        allow_unsupported: Skip unsupported types instead of failing
    """
    # diff_values: List of (key, value_a_hex, value_b_hex) tuples for differing values
    diff_values = []
    # diff_ttls: List of (key, ttl_a, ttl_b) tuples for differing TTLs
    diff_ttls = []
    
    if not keys:
        return diff_values, diff_ttls
    
    # Check values if requested
    if check_values:
        # Step 1: Pipeline TYPE commands for both databases to get Redis types
        pipe_a = a.pipeline(transaction=False)
        pipe_b = b.pipeline(transaction=False)
        for key in keys:
            pipe_a.type(key)
            pipe_b.type(key)
        # Execute TYPE commands (2 network round-trips total)
        types_a = pipe_a.execute()
        types_b = pipe_b.execute()
        
        # Step 2: Compare types and group keys by matching types
        # keys_by_type_a: Maps Redis type to list of (key, typ_b) tuples
        # Only includes keys where type matches in both databases
        keys_by_type_a: Dict[str, List[Tuple[str, str]]] = {}
        for key, typ_a, typ_b in zip(keys, types_a, types_b):
            if typ_a != typ_b:
                # Type mismatch - report as value difference with type info
                diff_values.append((key, f"type:{typ_a}", f"type:{typ_b}"))
            else:
                # Types match - group for batch value retrieval
                if typ_a not in keys_by_type_a:
                    keys_by_type_a[typ_a] = []
                keys_by_type_a[typ_a].append((key, typ_b))
        
        # Step 3: Fetch values from database A using pipeline
        # values_a: Maps key to canonical bytes representation
        values_a: Dict[str, bytes] = {}
        # For each type group, pipeline value fetch commands for database A
        for typ, key_pairs in keys_by_type_a.items():
            # Extract keys from (key, typ_b) tuples
            typ_keys = [k for k, _ in key_pairs]
            pipe = a.pipeline(transaction=False)
            
            # Add appropriate value-fetch command based on Redis type
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
                    # Unsupported type - use empty bytes
                    if allow_unsupported:
                        values_a[key] = b""
            
            # Execute pipeline for this type group
            if typ in ["string", "hash", "list", "set", "zset"]:
                results = pipe.execute()
                
                # Convert results to canonical bytes for comparison
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
        
        # Step 4: Fetch values from database B using pipeline (same logic as A)
        # values_b: Maps key to canonical bytes representation
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
        
        # Step 5: Compare canonical byte representations
        for key in values_a:
            if key in values_b:
                val_a = values_a[key]
                val_b = values_b[key]
                
                # Direct byte comparison of canonical representations
                if val_a != val_b:
                    # Store actual values as hex for binary-safe output
                    # Hex encoding prevents issues with non-printable bytes
                    diff_values.append((key, val_a.hex(), val_b.hex()))
    
    # Check TTL if requested
    if check_ttl:
        # Pipeline TTL commands for both databases
        pipe_a = a.pipeline(transaction=False)
        pipe_b = b.pipeline(transaction=False)
        for key in keys:
            pipe_a.ttl(key)
            pipe_b.ttl(key)
        # Execute TTL commands (2 network round-trips total)
        # TTL returns seconds until expiry, -1 for no expiry, -2 for non-existent key
        ttls_a = pipe_a.execute()
        ttls_b = pipe_b.execute()
        
        # Compare TTLs with tolerance window
        for key, ttl_a, ttl_b in zip(keys, ttls_a, ttls_b):
            # TTL semantics:
            # -1: Key exists but has no expiry (PERSIST)
            # -2: Key doesn't exist (shouldn't happen here as we checked existence)
            # >=0: Seconds until expiry
            
            if ttl_a == -1 and ttl_b == -1:
                # Both have no expiry - match
                continue
            elif ttl_a == -1 or ttl_b == -1:
                # One has expiry, one doesn't - fundamental mismatch
                diff_ttls.append((key, ttl_a, ttl_b))
            elif ttl_a >= 0 and ttl_b >= 0:
                # Both have expiry - check if difference exceeds tolerance
                # Tolerance accounts for replication lag and processing time
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
    Compare keys between A and B databases and report differences.
    
    Algorithm (two-pass):
    Pass 1 (A→B): Iterate keys in A, check existence in B
      - Keys not in B → ONLY_IN_A
      - Keys in both → optionally check values and TTLs
    Pass 2 (B→A): Iterate keys in B, check existence in A
      - Keys not in A → ONLY_IN_B
    
    Two-pass is necessary because:
    - Single pass can only find keys in source, not keys unique to target
    - Alternative would be loading all keys into memory (infeasible for large DBs)
    
    Memory-optimized: processes keys in batches to use at most ~(pipeline_batch*2) memory.
    
    Prints:
    - ONLY_IN_A: Keys existing only in database A
    - ONLY_IN_B: Keys existing only in database B
    - DIFF_VALUE: Keys with different values (if check_values=True)
    - DIFF_TTL: Keys with different TTLs (if check_ttl=True)
    
    Args:
        a: Redis connection A
        b: Redis connection B
        options: Options dict with scan_count, pipeline_batch, check_values, check_ttl, etc.
    """
    # Extract options for diff operation
    scan_count = options["scan_count"]
    pipeline_batch = options["pipeline_batch"]
    check_values = options["check_values"]
    check_ttl = options["check_ttl"]
    ttl_tolerance = options["ttl_tolerance"]
    allow_unsupported = options["allow_unsupported_types"]

    # Result accumulators
    only_in_a = []  # Keys found only in database A
    only_in_b = []  # Keys found only in database B
    diff_values = []  # List of (key, val_a_hex, val_b_hex) for value differences
    diff_ttls = []  # List of (key, ttl_a, ttl_b) for TTL differences

    try:
        # ======================================================================
        # PASS 1: A → B
        # ======================================================================
        # Iterate all keys in A and check if they exist in B
        # For keys in both, optionally compare values and TTLs
        sys.stderr.write("Scanning keys in database A...\n")
        sys.stderr.flush()
        
        keys_a_count = 0  # Total keys found in database A
        keys_in_both_count = 0  # Keys found in both A and B
        batch = []  # Current batch of keys being accumulated
        
        for key in iter_keys(a, scan_count):
            batch.append(key)
            keys_a_count += 1
            
            # Process batch when it reaches pipeline_batch size
            if len(batch) >= pipeline_batch:
                # Check which keys exist in B
                batch_only_in_a, batch_keys_in_both = _check_existence_batch(b, batch)
                only_in_a.extend(batch_only_in_a)
                keys_in_both_count += len(batch_keys_in_both)
                
                # For keys in both, optionally check values and TTL
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
                
                # Clear batch to free memory
                batch = []
        
        # Process remaining keys that didn't fill a complete batch
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
        
        # ======================================================================
        # PASS 2: B → A
        # ======================================================================
        # Iterate all keys in B and check if they exist in A
        # This finds keys unique to B (we already found keys unique to A in pass 1)
        # We don't need to check values/TTL here (already done in pass 1)
        sys.stderr.write("Scanning keys in database B...\n")
        sys.stderr.flush()
        
        keys_b_count = 0  # Total keys found in database B
        batch = []
        
        for key in iter_keys(b, scan_count):
            batch.append(key)
            keys_b_count += 1
            
            # Process batch when it reaches pipeline_batch size
            if len(batch) >= pipeline_batch:
                # Check which keys don't exist in A (only_in_b)
                # Discard keys_in_both (already processed in pass 1)
                batch_only_in_b, _ = _check_existence_batch(a, batch)
                only_in_b.extend(batch_only_in_b)
                
                # Progress reporting
                sys.stderr.write(f"[B→A] Processed {keys_b_count} keys from B\n")
                sys.stderr.flush()
                
                # Clear batch to free memory
                batch = []
        
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

    # ======================================================================
    # OUTPUT RESULTS
    # ======================================================================
    # Print all differences found in both passes
    # Results are printed to stdout in table format for easy parsing
    has_output = False
    
    # Print keys unique to database A
    if only_in_a:
        has_output = True
        print("\n" + "=" * 80)
        print("KEYS ONLY IN DATABASE A")
        print("=" * 80)
        print(f"{'Key':<78}")
        print("-" * 80)
        for key in only_in_a:
            print(f"{key:<78}")
    
    # Print keys unique to database B
    if only_in_b:
        has_output = True
        print("\n" + "=" * 80)
        print("KEYS ONLY IN DATABASE B")
        print("=" * 80)
        print(f"{'Key':<78}")
        print("-" * 80)
        for key in only_in_b:
            print(f"{key:<78}")
    
    # Print value differences (if check_values was enabled)
    if check_values and diff_values:
        has_output = True
        print("\n" + "=" * 80)
        print("VALUE DIFFERENCES")
        print("=" * 80)
        print(f"{'Key':<30} {'Value in A':<24} {'Value in B':<24}")
        print("-" * 80)
        for key, val_a, val_b in diff_values:
            # Truncate long hex values for display (full values in result lists)
            val_a_display = val_a[:22] + ".." if len(val_a) > 24 else val_a
            val_b_display = val_b[:22] + ".." if len(val_b) > 24 else val_b
            print(f"{key:<30} {val_a_display:<24} {val_b_display:<24}")
    
    # Print TTL differences (if check_ttl was enabled)
    if check_ttl and diff_ttls:
        has_output = True
        print("\n" + "=" * 80)
        print(f"TTL DIFFERENCES (tolerance: ±{ttl_tolerance}s)")
        print("=" * 80)
        print(f"{'Key':<50} {'TTL in A':<14} {'TTL in B':<14}")
        print("-" * 80)
        for key, ttl_a, ttl_b in diff_ttls:
            # Format TTL values: -1 means no expiry, >= 0 is seconds remaining
            ttl_a_str = "no_expiry" if ttl_a == -1 else f"{ttl_a}s"
            ttl_b_str = "no_expiry" if ttl_b == -1 else f"{ttl_b}s"
            print(f"{key:<50} {ttl_a_str:<14} {ttl_b_str:<14}")
    
    # Print footer separator if any output was generated
    if has_output:
        print("\n" + "=" * 80)


def mode_digest(
    a: redis.Redis | None,
    b: redis.Redis | None,
    options: Dict[str, Any],
    target: str,
) -> int:
    """
    Digest mode: compute order-independent digest hash of Redis database(s).
    
    Digest is a single 32-byte hash representing entire database contents.
    Use cases:
    - Quick check if databases are identical (compare digest hashes)
    - Verify replication/migration success
    - Change detection
    
    Target options:
    - 'a': Compute digest only for database A
    - 'b': Compute digest only for database B
    - 'both': Compute digests for both and compare
    
    Returns:
    - 0 if successful (or digests match for 'both')
    - 1 if digests don't match (for 'both')
    
    Args:
        a: Redis connection A (or None if not needed)
        b: Redis connection B (or None if not needed)
        options: Options dict
        target: Which database(s) to digest ('a', 'b', 'both')
    """
    if target == "a":
        # Compute digest for database A only
        if a is None:
            _die("Target 'a' specified but connection A not configured", code=2)
        # digest_a: 32-byte hash representing entire database A
        # stats_a: Dictionary with keys, bytes, unsupported counts
        digest_a, stats_a = digest_db(a, options, label="A")
        print(f"A_DIGEST {digest_a.hex()} keys={stats_a['keys']} bytes={stats_a['bytes']}")
        return 0

    elif target == "b":
        # Compute digest for database B only
        if b is None:
            _die("Target 'b' specified but connection B not configured", code=2)
        digest_b, stats_b = digest_db(b, options, label="B")
        print(f"B_DIGEST {digest_b.hex()} keys={stats_b['keys']} bytes={stats_b['bytes']}")
        return 0

    elif target == "both":
        # Compute digests for both databases and compare
        if a is None or b is None:
            _die("Target 'both' requires both A and B connections to be configured", code=2)
        
        sys.stderr.write("Computing digest for database A...\n")
        sys.stderr.flush()
        digest_a, stats_a = digest_db(a, options, label="A")
        
        sys.stderr.write("Computing digest for database B...\n")
        sys.stderr.flush()
        digest_b, stats_b = digest_db(b, options, label="B")

        # Convert digests to hex for human-readable output
        digest_a_hex = digest_a.hex()
        digest_b_hex = digest_b.hex()

        # Output both digests and comparison result
        print(f"A_DIGEST {digest_a_hex} keys={stats_a['keys']} bytes={stats_a['bytes']}")
        print(f"B_DIGEST {digest_b_hex} keys={stats_b['keys']} bytes={stats_b['bytes']}")

        # Compare digests - if identical, databases contain same data
        match = digest_a == digest_b
        print(f"DIGEST_MATCH {str(match).lower()}")

        # Exit code: 0 for match, 1 for mismatch
        return 0 if match else 1

    else:
        _die(f"Invalid target: {target!r} (must be 'a', 'b', or 'both')", code=2)


def mode_diff(a: redis.Redis, b: redis.Redis, options: Dict[str, Any]) -> int:
    """
    Diff mode: detailed comparison of keys between A and B.
    
    Reports:
    - Keys only in A
    - Keys only in B
    - Value differences (if check_values=True)
    - TTL differences (if check_ttl=True)
    
    More detailed than digest mode, but slower for large databases.
    Use when you need to know specifically which keys differ.
    
    Always returns 0 (use digest mode if you need non-zero exit for differences).
    
    Args:
        a: Redis connection A
        b: Redis connection B
        options: Options dict
    """
    diff_presence(a, b, options)
    return 0


def main(argv: List[str]) -> int:
    """
    Main entry point for redis_compare.py
    
    Workflow:
    1. Parse command-line arguments
    2. Determine which connections are needed
    3. Load YAML configuration
    4. Connect to Redis instance(s)
    5. Execute requested mode (digest or diff)
    
    Returns exit code:
    - 0: Success (or databases match in digest mode)
    - 1: Databases don't match (digest mode with --target both)
    - 2: Configuration/argument error
    - 3: Connection error
    - 4: Redis operation error
    
    Args:
        argv: Command-line arguments (sys.argv)
    """
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

    # Determine which connections are required based on mode and target
    # diff mode always needs both A and B
    # digest mode needs A and/or B depending on --target
    require_a = args.mode == "diff" or args.target in ["a", "both"]
    require_b = args.mode == "diff" or args.target in ["b", "both"]
    
    # Load YAML configuration file
    # Returns connection configs and options dict
    a_conf, b_conf, options = load_config(args.config, require_a=require_a, require_b=require_b)

    # Connect to Redis instances as needed
    # Connections are None if not required
    a = None
    b = None
    
    if require_a:
        a = connect(a_conf)
    
    if require_b:
        b = connect(b_conf)

    # Run selected mode and return exit code
    if args.mode == "digest":
        return mode_digest(a, b, options, args.target)
    elif args.mode == "diff":
        return mode_diff(a, b, options)
    else:
        _die(f"Unknown mode: {args.mode!r}", code=2)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))