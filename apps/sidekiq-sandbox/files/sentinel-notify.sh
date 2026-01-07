#!/bin/bash

set -euo pipefail

base="${PERSISTENT_MOUNT_PATH:-/data}"
log_file="${base}/redis-sentinel/log/sentinel-notify.log"

mkdir -p "$(dirname "$log_file")"
echo "$(date '+%Y-%m-%d %H:%M:%S') - Sentinel Event: $*" | tee -a "$log_file"


