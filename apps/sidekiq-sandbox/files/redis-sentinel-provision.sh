#!/bin/bash

set -euo pipefail

# Common functions

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
error_if_env_not_set REDIS_MASTER_CLUSTER_DNS_NAME

# Sysctl

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

mkdir -p "$PERSISTENT_MOUNT_PATH/redis-sentinel/data"
mkdir -p "$PERSISTENT_MOUNT_PATH/redis-sentinel/log"
mkdir -p "$PERSISTENT_MOUNT_PATH/redis-sentinel/run"
mkdir -p "$PERSISTENT_MOUNT_PATH/redis-sentinel/tmp"

touch "$PERSISTENT_MOUNT_PATH/redis-sentinel/log/redis-sentinel.log"
touch "$PERSISTENT_MOUNT_PATH/redis-sentinel/log/sentinel-notify.log"

# Redis Sentinel

tee "$PERSISTENT_MOUNT_PATH/redis-sentinel/sentinel.conf" >/dev/null <<EOF
port 26379
dir $PERSISTENT_MOUNT_PATH/redis-sentinel/tmp
logfile $PERSISTENT_MOUNT_PATH/redis-sentinel/log/redis-sentinel.log
requirepass $REDIS_PASSWORD
sentinel resolve-hostnames yes

sentinel down-after-milliseconds cluster_name 6000
sentinel failover-timeout cluster_name 18000
sentinel auth-pass cluster_name $REDIS_PASSWORD
sentinel notification-script cluster_name /scripts/sentinel-notify.sh
sentinel monitor cluster_name $REDIS_MASTER_CLUSTER_DNS_NAME 6379 2

pidfile $PERSISTENT_MOUNT_PATH/redis-sentinel/run/redis-sentinel.pid
daemonize no
EOF