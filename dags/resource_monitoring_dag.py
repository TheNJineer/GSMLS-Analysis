"""Monitor host resource usage for the GSMLS platform.

The DAG observes resources only. It does not prune Docker data, stop containers,
resize services, or otherwise change the host.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.exceptions import AirflowException
from airflow.sdk import dag, task

try:
    from airflow.providers.ssh.operators.ssh import SSHOperator
except ImportError:
    class SSHOperator:  # type: ignore[no-redef]
        """Fallback stub for SSHOperator when airflow-providers-ssh is not installed locally."""
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.output = ""


logger = logging.getLogger(__name__)

# Default connection ID used to connect to the target host
DEFAULT_SSH_CONN_ID = os.getenv("RESOURCE_MONITOR_SSH_CONN_ID", "ssh_host_conn")

# Path to the shell collector script (resolved dynamically relative to this DAG file)
SCRIPT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "jobs", "major_jobs", "resource_monitor_shell.sh")
)


def _load_collector_script() -> str:
    """Read collector script content to execute remotely on target host."""
    try:
        if os.path.exists(SCRIPT_PATH):
            with open(SCRIPT_PATH, "r", encoding="utf-8") as file:
                return file.read()
        logger.warning("Collector script not found at local path %s", SCRIPT_PATH)
    except Exception as err:
        logger.warning("Could not read local collector script from %s: %s", SCRIPT_PATH, err)
    return "bash /opt/airflow/jobs/major_jobs/resource_monitor_shell.sh"


def _number(value: str | float | int | None, default: float = 0.0) -> float:
    """Convert a shell value to a number while tolerating unavailable data."""
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def parse_host_snapshot(raw_output: str | bytes | dict[str, Any] | None) -> dict[str, Any]:
    """Parse JSON or key=value records emitted by resource collector, handling base64 XCom payloads."""
    if raw_output is None:
        data: dict[str, Any] = {}
    elif isinstance(raw_output, dict):
        data = raw_output
    else:
        if isinstance(raw_output, bytes):
            text = raw_output.decode("utf-8", errors="replace")
        else:
            text = str(raw_output).strip()

        # Handle base64 decoding if SSHOperator pushed base64 encoded stdout
        try:
            decoded = base64.b64decode(text, validate=True).decode("utf-8", errors="replace")
            if decoded.strip().startswith("{") or "=" in decoded:
                text = decoded.strip()
        except Exception:
            pass

        try:
            # Extract the last non-empty line in case SSH banner or MOTD precedes the payload
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            last_line = lines[-1] if lines else "{}"
            data = json.loads(last_line)
        except Exception as exc:
            logger.warning("Failed to parse JSON host snapshot: %s. Raw output: %s", exc, raw_output)
            data = {}
            for line in text.splitlines():
                if "=" in line:
                    key, value = line.split("=", 1)
                    data[key.strip()] = value.strip()

    return {
        "timestamp": data.get("timestamp"),
        "load_1m": _number(data.get("load_1m")),
        "load_5m": _number(data.get("load_5m")),
        "load_15m": _number(data.get("load_15m")),
        "memory_total_kb": _number(data.get("memory_total_kb")),
        "memory_available_kb": _number(data.get("memory_available_kb")),
        "memory_used_pct": _number(data.get("memory_used_pct")),
        "disk_available_kb": _number(data.get("disk_available_kb")),
        "disk_used_pct": _number(data.get("disk_used_pct")),
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

    default_settings = f"""
    
    Default Resource Monitor Settings:
    vCPUs: {vcpus}
    vCPU Load Warning Level: {load_warning} 
    vCPU Load Critical Level: {load_critical}
    Memory Usage Warning Pct: {memory_warning}%
    Memory Usage Critical Pct: {memory_critical}%
    Disk Usage Warning Pct: {disk_warning}%
    Disk Usage Critical Pct: {disk_critical}%
    """

    print(default_settings)

    report = {
        "timestamp": host.get("timestamp"),
        "host": host,
        "warnings": warnings,
        "critical": critical,
    }

    # logger.info("Resource report: %s", json.dumps(report, sort_keys=True))

    if len(warnings) > 0:
        for message in warnings:
            logger.warning(f"==== {message.upper()} ==== ")
    else:
        logger.info(" ==== NO RESOURCE WARNINGS DETECTED ==== ")

    if len(critical) > 0:
        for message in critical:
            logger.error(f"==== {message.upper()} ==== ")
    else:
        logger.info(" ==== NO CRITICAL RESOURCE WARNINGS DETECTED ==== ")

    print(report['timestamp'])
    print(report['host'])

    # if critical:
    #     raise AirflowException("Critical resource threshold exceeded: " + "; ".join(critical))


@dag(
    dag_id="GSMLS_Resource_Monitoring",
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
    host_snapshot = SSHOperator(
        task_id="collect_host_resources",
        ssh_conn_id=DEFAULT_SSH_CONN_ID,
        command=_load_collector_script(),
        do_xcom_push=True,
    )

    @task(task_id="evaluate_resource_usage")
    def evaluate(host_raw: str) -> dict[str, Any]:
        return evaluate_resources(parse_host_snapshot(host_raw))

    evaluate(host_snapshot.output)


gsmls_resource_monitoring()
