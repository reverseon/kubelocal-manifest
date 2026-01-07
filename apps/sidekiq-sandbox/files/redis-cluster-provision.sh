#!/bin/bash

set -euo pipefail

# Common functions

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

error_if_env_not_set() {
  local env_var="$1"
  if [ -z "${!env_var}" ]; then
    echo "Error: $env_var is not set"
    exit 1
  fi
}

# Env sanity checks

error_if_env_not_set PERSISTENT_MOUNT_PATH
error_if_env_not_set REDIS_PASSWORD
error_if_env_not_set REDIS_MASTER_HOST_NAME

# Sysctl
# Commented out - these are now configured via Kubernetes securityContext.sysctls

# dest="/etc/sysctl.d/00-redis.conf"

# sudo tee "$dest" >/dev/null <<'EOF'
# kernel.msgmnb = 65536
# kernel.msgmax = 65536
# kernel.sem = 1000 32000 100 128
# kernel.panic = 1
# kernel.panic_on_unrecovered_nmi = 1

# net.core.rmem_max = 1572864
# net.core.wmem_max = 524288
# net.core.somaxconn = 512

# net.ipv4.tcp_mem = 196608 262144 393216
# net.ipv4.tcp_rmem = 16384 131072 10485760
# net.ipv4.tcp_wmem = 49152 262144 10485760

# net.ipv4.ip_forward = 0

# net.ipv6.conf.all.disable_ipv6 = 1
# net.ipv6.conf.default.disable_ipv6 = 1

# fs.file-max = 359121

# vm.swappiness = 1
# vm.overcommit_ratio = 99
# vm.overcommit_memory = 1
# EOF

# sudo chown root:root "$dest"
# sudo chmod 0644 "$dest"
# sudo sysctl --system

# Create Directories

mkdir -p "$PERSISTENT_MOUNT_PATH/redis/data"
mkdir -p "$PERSISTENT_MOUNT_PATH/redis/log"
mkdir -p "$PERSISTENT_MOUNT_PATH/redis/run"
mkdir -p "$PERSISTENT_MOUNT_PATH/redis/tmp"

# Redis

current_hostname="$(hostname -s 2>/dev/null || hostname)"
replicaof_line=""
if [ "$current_hostname" != "$REDIS_MASTER_HOST_NAME" ]; then
  replicaof_line="replicaof $REDIS_MASTER_HOST_NAME 6379"
fi

tee "$PERSISTENT_MOUNT_PATH/redis/redis.conf" >/dev/null <<EOF
# https://raw.githubusercontent.com/redis/redis/6.2/redis.conf

## Common Configs ##
tcp-backlog 511
timeout 3600
tcp-keepalive 300
daemonize no
databases 16
maxclients 10000
save 600 1
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
replica-serve-stale-data yes
replica-read-only yes
repl-diskless-sync no
repl-diskless-sync-delay 5
repl-disable-tcp-nodelay no
replica-priority 100
min-replicas-max-lag 10
tracking-table-max-keys 1000000
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
aof-load-truncated yes
aof-rewrite-incremental-fsync yes
aof-use-rdb-preamble yes
replica-ignore-maxmemory yes
active-expire-effort 1
lazyfree-lazy-eviction no
lazyfree-lazy-expire no
lazyfree-lazy-server-del no
replica-lazy-flush no
lazyfree-lazy-user-del no
lazyfree-lazy-user-flush no
io-threads 1
oom-score-adj no
disable-thp yes
lua-time-limit 5000
slowlog-log-slower-than 10000
slowlog-max-len 128
latency-monitor-threshold 0
notify-keyspace-events ""
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-entries 512
list-max-ziplist-value 64
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
hll-sparse-max-bytes 3000
activerehashing yes
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit replica 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60
hz 10

## Redis Cluster Configs ##
daemonize no
bind 0.0.0.0
port 6379
$replicaof_line
protected-mode no
pidfile $PERSISTENT_MOUNT_PATH/redis/run/redis.pid

requirepass $REDIS_PASSWORD
masterauth $REDIS_PASSWORD

loglevel notice
# logfile $PERSISTENT_MOUNT_PATH/redis/log/redis.log
logfile "" # to stdout

appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec

dir $PERSISTENT_MOUNT_PATH/redis/data/
dbfilename redis.rdb

min-replicas-to-write 2
EOF

# Disable THP (requires privileged/root; intentionally skipped for non-root pods)