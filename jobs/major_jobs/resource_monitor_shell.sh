#!/usr/bin/env bash

# Host-level resource collector for resource_monitoring_dag.py.
# This script intentionally reports information only; it does not modify the host.

set -u

echo "timestamp=$(date --iso-8601=seconds)"

if [ -r /proc/loadavg ]; then
    read -r load_1 load_5 load_15 _ < /proc/loadavg
    echo "load_1m=${load_1}"
    echo "load_5m=${load_5}"
    echo "load_15m=${load_15}"
else
    echo "load_status=unavailable"
fi

if [ -r /proc/meminfo ]; then
    mem_total_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
    mem_available_kb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)

    if [ -n "${mem_total_kb}" ] && [ "${mem_total_kb}" -gt 0 ]; then
        mem_used_pct=$(awk -v total="${mem_total_kb}" -v available="${mem_available_kb}" \
            'BEGIN {printf "%.2f", ((total - available) / total) * 100}')
        echo "memory_total_kb=${mem_total_kb}"
        echo "memory_available_kb=${mem_available_kb}"
        echo "memory_used_pct=${mem_used_pct}"
    else
        echo "memory_status=unavailable"
    fi
else
    echo "memory_status=unavailable"
fi

disk_line=$(df -P / | awk 'NR == 2 {print $4 " " $5}')
if [ -n "${disk_line}" ]; then
    read -r disk_available_kb disk_used_pct <<< "${disk_line}"
    disk_used_pct=${disk_used_pct%\%}
    echo "disk_available_kb=${disk_available_kb}"
    echo "disk_used_pct=${disk_used_pct}"
else
    echo "disk_status=unavailable"
fi
