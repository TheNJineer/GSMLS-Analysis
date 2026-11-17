#!/usr/bin/env python3
"""Read-only inventory for the guarded system cleanup DAG task."""

from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

MIN_RECLAIMABLE_KB = 5_242_880
MAX_ITEMS = 200
SCAN_ROOTS = ("/", "/var", "/opt", "/root", "/home", "/tmp", "/var/lib")
PROTECTED_MARKERS = (
    "/etc", "/usr", "/bin", "/sbin", "/lib", "/boot", "/dev", "/proc", "/sys",
    "/run", "/var/lib/docker", "/var/lib/postgresql", "/var/lib/mongodb", "/var/lib/redis",
    "/var/lib/kafka", "/opt/airflow", "/dags", "/logs", "/secrets", "/.git",
)


def command(*args: str) -> tuple[str, bool]:
    try:
        result = subprocess.run(args, check=False, capture_output=True, text=True, timeout=15)
        return result.stdout.strip(), result.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return "", False


def size_kb(path: Path) -> int:
    try:
        return max(0, (path.stat().st_size + 1023) // 1024)
    except OSError:
        return 0


def protected(path: Path) -> bool:
    value = "/" + str(path).lstrip("/")
    return any(value == marker or value.startswith(marker.rstrip("/") + "/") for marker in PROTECTED_MARKERS)


def candidate(resource: str, size: int, filesystem: str, action: str) -> dict[str, Any]:
    return {
        "resource": resource,
        "size_kb": max(0, int(size)),
        "filesystem": filesystem,
        "eligibility": "reclaimable",
        "action": action,
        "protection_reason": "explicitly disposable cache or temporary artifact",
    }


def filesystem_metrics(warnings: list[str]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for path in ("/", "/var", "/tmp"):
        try:
            usage = shutil.disk_usage(path)
            metrics[path] = {"total_kb": usage.total // 1024, "used_kb": (usage.total - usage.free) // 1024,
                             "available_kb": usage.free // 1024}
        except OSError as exc:
            warnings.append(f"filesystem metrics unavailable for {path}: {exc}")
    return metrics


def disposable_files(warnings: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    protected_resources: list[dict[str, Any]] = []
    now = time.time()
    roots = (Path("/tmp"), Path("/var/tmp"), Path("/var/cache/apt/archives"), Path("/root/.cache/pip"),
             Path.home() / ".cache/pip")
    for root in roots:
        if not root.is_dir() or protected(root):
            continue
        try:
            for entry in root.rglob("*"):
                if len(candidates) >= MAX_ITEMS:
                    warnings.append("candidate inventory bounded at 200 items")
                    return candidates, protected_resources
                if not entry.is_file() or entry.is_symlink():
                    continue
                age_days = (now - entry.stat().st_mtime) / 86400
                if age_days < 7:
                    continue
                size = size_kb(entry)
                if size:
                    candidates.append(candidate(str(entry), size, "/", "remove stale disposable cache/temp file"))
        except OSError as exc:
            warnings.append(f"read-only scan unavailable for {root}: {exc}")
    return candidates, protected_resources


def docker_inventory(warnings: list[str]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metrics: dict[str, Any] = {}
    candidates: list[dict[str, Any]] = []
    if not shutil.which("docker"):
        warnings.append("Docker CLI unavailable; Docker inventory skipped")
        return metrics, candidates
    output, ok = command("docker", "system", "df", "--format", "{{json .}}")
    if ok:
        rows = []
        for line in output.splitlines()[:50]:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                warnings.append("Docker system df returned malformed metadata")
        metrics["system_df"] = rows
    else:
        warnings.append("Docker daemon or system df unavailable")
        return metrics, candidates

    images, ok = command("docker", "image", "ls", "--filter", "dangling=true", "--format", "{{.ID}}")
    containers, containers_ok = command("docker", "ps", "-aq")
    referenced: set[str] = set()
    if containers_ok:
        for container in containers.splitlines():
            image, inspect_ok = command("docker", "inspect", "--format", "{{.Image}}", container)
            if not inspect_ok:
                warnings.append(f"Docker container {container} could not be inspected; image cleanup is fail-closed")
                return metrics, candidates
            referenced.add(image)
    if not ok or not containers_ok:
        warnings.append("Docker image/container inventory incomplete; image cleanup is fail-closed")
        return metrics, candidates
    for image_id in images.splitlines():
        if image_id and image_id not in referenced:
            raw_size, inspect_ok = command("docker", "image", "inspect", "--format", "{{.Size}}", image_id)
            if not inspect_ok or not raw_size.isdigit():
                warnings.append(f"Docker image {image_id} size could not be verified; image is protected")
                continue
            candidates.append(candidate(f"docker image {image_id}", int(raw_size) // 1024, "docker", "remove unreferenced dangling image"))
    return metrics, candidates


def main() -> int:
    warnings: list[str] = []
    candidates, protected_resources = disposable_files(warnings)
    docker_metrics, docker_candidates = docker_inventory(warnings)
    candidates.extend(docker_candidates)
    total = sum(item["size_kb"] for item in candidates)
    result = {
        "status": "OK" if not warnings else "UNSAFE",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "threshold_kb": MIN_RECLAIMABLE_KB,
        "threshold_met": total >= MIN_RECLAIMABLE_KB,
        "skip_rationale": "threshold met" if total >= MIN_RECLAIMABLE_KB else "reclaimable space is below 5 GiB",
        "scanned_directories": list(SCAN_ROOTS),
        "filesystem": filesystem_metrics(warnings),
        "docker": docker_metrics,
        "candidates": candidates[:MAX_ITEMS],
        "protected_resources": protected_resources + [
            {"resource": marker, "eligibility": "protected", "protection_reason": "host/runtime or persistent application state"}
            for marker in PROTECTED_MARKERS
        ],
        "warnings": warnings,
        "total_reclaimable_kb": max(0, total),
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())