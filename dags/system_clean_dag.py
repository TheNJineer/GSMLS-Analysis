"""Scheduled host maintenance and system cleanup pipeline for the GSMLS platform.

Orchestrates jobs/major_jobs/system_clean.sh on the target host to safely purge
ephemeral logs, temporary scraper artifacts, package caches, and system journals
while strictly preserving business logic, DAG scripts, database volumes, and configurations.
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

# Default SSH connection ID used to connect to the target host
DEFAULT_SSH_CONN_ID = os.getenv("SYSTEM_CLEAN_SSH_CONN_ID", "ssh_host_conn")

# Path to the shell cleanup script (resolved dynamically relative to this DAG file)
SCRIPT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "jobs", "major_jobs", "system_clean.sh")
)


def _load_cleanup_command() -> str:
    """Read cleanup script content to stream and execute remotely via SSH."""
    try:
        if os.path.exists(SCRIPT_PATH):
            with open(SCRIPT_PATH, "r", encoding="utf-8") as file:
                return file.read()
        logger.warning("System clean script not found at local path %s", SCRIPT_PATH)
    except Exception as err:
        logger.warning("Could not read local system clean script from %s: %s", SCRIPT_PATH, err)
    return "bash /opt/airflow/jobs/major_jobs/system_clean.sh"


def _number(value: str | float | int | None, default: float = 0.0) -> float:
    """Convert a shell value to a number while tolerating unavailable data."""
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def parse_cleanup_output(raw_output: str | bytes | dict[str, Any] | None) -> dict[str, Any]:
    """Parse JSON or key=value record emitted by system_clean.sh."""
    if raw_output is None:
        return {}
    if isinstance(raw_output, dict):
        return raw_output

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
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        last_line = lines[-1] if lines else "{}"
        data = json.loads(last_line)
    except Exception as exc:
        logger.warning("Failed to parse JSON summary from system_clean.sh: %s. Output: %s", exc, raw_output)
        data = {}
        for line in text.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip()

    return {
        "timestamp": data.get("timestamp"),
        "status": data.get("status", "UNKNOWN"),
        "duration_sec": _number(data.get("duration_sec")),
        "dry_run": bool(data.get("dry_run", False)),
        "is_emergency": bool(data.get("is_emergency", False)),
        "initial_available_kb": _number(data.get("initial_available_kb")),
        "initial_used_pct": _number(data.get("initial_used_pct")),
        "final_available_kb": _number(data.get("final_available_kb")),
        "final_used_pct": _number(data.get("final_used_pct")),
        "reclaimed_kb": _number(data.get("reclaimed_kb")),
        "categories_scanned": _number(data.get("categories_scanned")),
    }


def evaluate_cleanup_results(metrics: dict[str, Any]) -> dict[str, Any]:
    """Log structured execution summary and assess post-cleanup host disk health."""
    reclaimed_mb = metrics["reclaimed_kb"] / 1024.0
    final_used_pct = metrics["final_used_pct"]
    initial_used_pct = metrics["initial_used_pct"]
    duration = metrics["duration_sec"]
    dry_run = metrics["dry_run"]
    mode = "DRY-RUN" if dry_run else ("EMERGENCY" if metrics["is_emergency"] else "ROUTINE")

    logger.info("==================================================")
    logger.info("SYSTEM CLEANUP SUMMARY [%s]", mode)
    logger.info("Status:              %s", metrics.get("status"))
    logger.info("Duration:            %.1fs", duration)
    logger.info("Initial Disk Usage:  %.1f%%", initial_used_pct)
    logger.info("Final Disk Usage:    %.1f%%", final_used_pct)
    logger.info("Estimated Reclaimed: %.2f MB", reclaimed_mb)
    logger.info("Categories Scanned:  %d", int(metrics["categories_scanned"]))
    logger.info("==================================================")

    critical_threshold = _number(os.getenv("SYSTEM_CLEAN_CRITICAL_THRESHOLD"), 90.0)
    warning_threshold = _number(os.getenv("SYSTEM_CLEAN_WARNING_THRESHOLD"), 80.0)

    warnings: list[str] = []
    critical: list[str] = []

    if final_used_pct >= critical_threshold:
        critical.append(f"Post-cleanup disk usage {final_used_pct:.1f}% exceeds critical threshold ({critical_threshold:.1f}%)")
    elif final_used_pct >= warning_threshold:
        warnings.append(f"Post-cleanup disk usage {final_used_pct:.1f}% exceeds warning threshold ({warning_threshold:.1f}%)")

    for warn in warnings:
        logger.warning("CLEANUP WARNING: %s", warn)
    for crit in critical:
        logger.error("CLEANUP CRITICAL: %s", crit)

    if critical and not dry_run:
        raise AirflowException("Host disk remains in critical state after cleanup: " + "; ".join(critical))

    return {
        "metrics": metrics,
        "warnings": warnings,
        "critical": critical,
    }


@dag(
    dag_id="GSMLS_System_Cleanup",
    schedule="0 3 * * *",  # Daily at 3:00 AM EST
    start_date=pendulum.datetime(2026, 9, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Jibreel Hameed",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email": ["nj.realestate.pybot@gmail.com"],
        "email_on_failure": True,
    },
    description="Automated host maintenance and safe ephemeral file cleanup pipeline.",
    tags=["gsmls", "maintenance", "cleanup", "system"],
)
def gsmls_system_cleanup():
    cleanup_execution = SSHOperator(
        task_id="execute_system_clean",
        ssh_conn_id=DEFAULT_SSH_CONN_ID,
        command=_load_cleanup_command(),
        do_xcom_push=True,
    )

    @task(task_id="process_cleanup_results")
    def process_results(raw_output: str) -> dict[str, Any]:
        metrics = parse_cleanup_output(raw_output)
        return evaluate_cleanup_results(metrics)

    process_results(cleanup_execution.output)


gsmls_system_cleanup()
