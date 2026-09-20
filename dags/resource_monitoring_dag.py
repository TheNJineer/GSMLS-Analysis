"""Monitor host resource usage for the GSMLS platform.

The DAG observes resources only. It does not prune Docker data, stop containers,
resize services, or otherwise change the host.
"""

from __future__ import annotations

import logging
import os
import json
from datetime import timedelta
from typing import Any

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


logger = logging.getLogger(__name__)


def _number(value: str | None, default: float = 0.0) -> float:
    """Convert a shell value to a number while tolerating unavailable data."""

    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def parse_host_snapshot(raw_output: str) -> dict[str, Any]:
    """Parse key=value records emitted by resource_monitor_shell.sh."""

    values: dict[str, str] = {}
    for line in raw_output.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    return {
        "timestamp": values.get("timestamp"),
        "load_1m": _number(values.get("load_1m")),
        "load_5m": _number(values.get("load_5m")),
        "load_15m": _number(values.get("load_15m")),
        "memory_total_kb": _number(values.get("memory_total_kb")),
        "memory_available_kb": _number(values.get("memory_available_kb")),
        "memory_used_pct": _number(values.get("memory_used_pct")),
        "disk_available_kb": _number(values.get("disk_available_kb")),
        "disk_used_pct": _number(values.get("disk_used_pct")),
    }


def evaluate_resources(host: dict[str, Any]) -> dict[str, Any]:
    """Evaluate host resources using optional execution-time environment variables.

    The RESOURCE_MONITOR_* variables are not required to exist in the operating
    system, Docker Compose file, or Airflow environment. If they are absent,
    the defaults defined below are assigned when this function executes. The
    resolved values are printed before warning and critical messages are logged.
    """

    vcpus = _number(os.getenv("RESOURCE_MONITOR_VCPUS"), 4.0)
    load_warning = _number(os.getenv("RESOURCE_MONITOR_LOAD_WARNING"), vcpus * 0.75)
    load_critical = _number(os.getenv("RESOURCE_MONITOR_LOAD_CRITICAL"), vcpus * 1.25)
    memory_warning = _number(os.getenv("RESOURCE_MONITOR_MEMORY_WARNING"), 80.0)
    memory_critical = _number(os.getenv("RESOURCE_MONITOR_MEMORY_CRITICAL"), 95.0)
    disk_warning = _number(os.getenv("RESOURCE_MONITOR_DISK_WARNING"), 80.0)
    disk_critical = _number(os.getenv("RESOURCE_MONITOR_DISK_CRITICAL"), 90.0)
    warnings: list[str] = []
    critical: list[str] = []

    load_1m = _number(host.get("load_1m"))
    memory_used = _number(host.get("memory_used_pct"))
    disk_used = _number(host.get("disk_used_pct"))

    if load_1m >= load_critical:
        critical.append(f"host 1-minute load {load_1m:.2f} >= {load_critical:.2f}")
    elif load_1m >= load_warning:
        warnings.append(f"host 1-minute load {load_1m:.2f} >= {load_warning:.2f}")

    if memory_used >= memory_critical:
        critical.append(f"host memory usage {memory_used:.2f}% >= {memory_critical:.2f}%")
    elif memory_used >= memory_warning:
        warnings.append(f"host memory usage {memory_used:.2f}% >= {memory_warning:.2f}%")

    if disk_used >= disk_critical:
        critical.append(f"root disk usage {disk_used:.2f}% >= {disk_critical:.2f}%")
    elif disk_used >= disk_warning:
        warnings.append(f"root disk usage {disk_used:.2f}% >= {disk_warning:.2f}%")

    report = {
        "timestamp": host.get("timestamp"),
        "host": host,
        "warnings": warnings,
        "critical": critical,
    }

    print("Resolved resource monitor settings:")
    print(f"  vCPUs: {vcpus}")
    print(f"  load warning / critical: {load_warning} / {load_critical}")
    print(f"  memory warning / critical: {memory_warning}% / {memory_critical}%")
    print(f"  disk warning / critical: {disk_warning}% / {disk_critical}%")

    logger.info("Resource report: %s", json.dumps(report, sort_keys=True))

    for message in warnings:
        logger.warning(message)
    for message in critical:
        logger.error(message)

    if critical:
        raise AirflowException("Critical resource threshold exceeded: " + "; ".join(critical))

    return report


@dag(
    dag_id="gsmls_resource_monitoring",
    schedule=timedelta(minutes=15),
    start_date=pendulum.datetime(2026, 8, 27, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Jibreel Hameed",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "email": ["nj.realestate.pybot@gmail.com"],
        "email_on_failure": True,
    },
    description="Read-only host resource monitoring for the GSMLS platform.",
    tags=["gsmls", "monitoring", "resources"],
)
def gsmls_resource_monitoring():
    host_snapshot = BashOperator(
        task_id="collect_host_resources",
        bash_command="bash /opt/airflow/dags/resource_monitor_shell.sh",
        do_xcom_push=True,
    )

    @task(task_id="evaluate_resource_usage")
    def evaluate(host_raw: str) -> dict[str, Any]:
        return evaluate_resources(parse_host_snapshot(host_raw))

    evaluate(host_snapshot.output)


gsmls_resource_monitoring()
