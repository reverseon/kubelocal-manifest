#!/bin/bash
# Redis Sentinel notification script
# Called when failover events occur

EVENT_TYPE=$1
EVENT_NAME=$2
shift 2
EVENT_ARGS="$@"

echo "$(date '+%Y-%m-%d %H:%M:%S') - Sentinel Event: $EVENT_TYPE $EVENT_NAME $EVENT_ARGS" | tee -a /etc/redis/sentinel-notify.log