#!/usr/bin/env bash

# Host-level resource collector for resource_monitoring_dag.py.
# This script intentionally reports information only; it does not modify the host.

set -u

timestamp=$(date --iso-8601=seconds)
load_1=0.0; load_5=0.0; load_15=0.0
mem_total_kb=0; mem_available_kb=0; mem_used_pct=0.0
disk_available_kb=0; disk_used_pct=0.0

if [ -r /proc/loadavg ]; then
    read -r load_1 load_5 load_15 _ < /proc/loadavg
fi

if [ -r /proc/meminfo ]; then
    mem_total_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo || echo 0)
    mem_available_kb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo || echo 0)
    if [ -n "${mem_total_kb}" ] && [ "${mem_total_kb}" -gt 0 ]; then
        mem_used_pct=$(awk -v total="${mem_total_kb}" -v available="${mem_available_kb}" \
            'BEGIN {printf "%.2f", ((total - available) / total) * 100}')
    fi
fi

disk_line=$(df -P / 2>/dev/null | awk 'NR == 2 {print $4 " " $5}')
if [ -n "${disk_line}" ]; then
    read -r disk_available_kb disk_used_pct <<< "${disk_line}"
    disk_used_pct=${disk_used_pct%\%}
fi

cat <<EOF
{"timestamp":"${timestamp}","load_1m":${load_1:-0.0},"load_5m":${load_5:-0.0},"load_15m":${load_15:-0.0},"memory_total_kb":${mem_total_kb:-0},"memory_available_kb":${mem_available_kb:-0},"memory_used_pct":${mem_used_pct:-0.0},"disk_available_kb":${disk_available_kb:-0},"disk_used_pct":${disk_used_pct:-0.0}}
EOF
