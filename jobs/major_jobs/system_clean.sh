#!/usr/bin/env bash
# ==============================================================================
# Script: system_clean.sh
# Purpose: Safe, tiered host system cleanup and maintenance script.
# Orchestrated by Apache Airflow (via SSHOperator/BashOperator) or run manually.
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# Configuration & Default Parameters
# ------------------------------------------------------------------------------
DRY_RUN=false
FORCE_EMERGENCY=false
EMERGENCY_USED_PCT_THRESHOLD=85
EMERGENCY_AVAIL_KB_THRESHOLD=5242880 # 5 GB in KB
TARGET_MOUNT="/"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" 2>/dev/null && pwd || pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-}"
if [ -z "${PROJECT_ROOT}" ]; then
    if [ -d "/root/home/projects/GSMLS-Analysis" ]; then
        PROJECT_ROOT="/root/home/projects/GSMLS-Analysis"
    elif [ -d "/opt/airflow" ]; then
        PROJECT_ROOT="/opt/airflow"
    else
        PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." 2>/dev/null && pwd || pwd)"
    fi
fi

# Cumulative metric counters (in KB)
TOTAL_RECLAIMED_KB=0
PROCESSED_CATEGORIES=0

# ------------------------------------------------------------------------------
# Usage & Help
# ------------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Safe, tiered host system cleanup script for disk optimization and log pruning.

Options:
  -d, --dry-run                 Simulate cleanup and report candidate files and estimated space savings.
  -f, --force-emergency, -e     Force emergency (aggressive) cleanup tier regardless of disk usage.
  -t, --threshold <percent>     Disk usage percentage threshold to trigger emergency cleanup (default: ${EMERGENCY_USED_PCT_THRESHOLD}%).
  -p, --project-dir <path>      Path to project repository root (default: ${PROJECT_ROOT}).
  -m, --mount <path>            Filesystem mount point to evaluate (default: ${TARGET_MOUNT}).
  -h, --help                    Display this help message and exit.

EOF
    exit 0
}

# ------------------------------------------------------------------------------
# Argument Parsing
# ------------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -f|--force-emergency|-e|--emergency)
            FORCE_EMERGENCY=true
            shift
            ;;
        -t|--threshold)
            if [[ -n "${2:-}" && "$2" =~ ^[0-9]+$ ]]; then
                EMERGENCY_USED_PCT_THRESHOLD="$2"
                shift 2
            else
                echo "Error: --threshold requires an integer percentage (e.g. 85)." >&2
                exit 1
            fi
            ;;
        -p|--project-dir)
            if [[ -n "${2:-}" && -d "$2" ]]; then
                PROJECT_ROOT="$(cd "$2" && pwd)"
                shift 2
            else
                echo "Error: --project-dir requires a valid directory path." >&2
                exit 1
            fi
            ;;
        -m|--mount)
            if [[ -n "${2:-}" && -d "$2" ]]; then
                TARGET_MOUNT="$2"
                shift 2
            else
                echo "Error: --mount requires a valid directory/mount path." >&2
                exit 1
            fi
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Run '$(basename "$0") --help' for usage." >&2
            exit 1
            ;;
    esac
done

# ------------------------------------------------------------------------------
# Helper Utilities & Metrics
# ------------------------------------------------------------------------------
get_disk_metrics() {
    local target="$1"
    local df_output
    df_output=$(df -P "${target}" 2>/dev/null | awk 'NR == 2 {print $2, $3, $4, $5}')
    if [[ -z "${df_output}" ]]; then
        echo "0 0 0 0"
        return
    fi
    local total_kb used_kb avail_kb used_pct_raw
    read -r total_kb used_kb avail_kb used_pct_raw <<< "${df_output}"
    local used_pct="${used_pct_raw%\%}"
    echo "${total_kb:-0} ${used_kb:-0} ${avail_kb:-0} ${used_pct:-0}"
}

format_kb() {
    local kb="$1"
    if ! [[ "$kb" =~ ^-?[0-9]+$ ]]; then
        echo "${kb} KB"
        return
    fi
    if [ "$kb" -lt 0 ]; then
        echo "0 MB"
        return
    fi
    if [ "$kb" -ge 1048576 ]; then
        awk -v k="$kb" 'BEGIN {printf "%.2f GB", k/1048576}'
    elif [ "$kb" -ge 1024 ]; then
        awk -v k="$kb" 'BEGIN {printf "%.2f MB", k/1024}'
    else
        echo "${kb} KB"
    fi
}

print_preview_list() {
    local text="$1"
    local max="${2:-5}"
    local count=0
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        if [ "$count" -lt "$max" ]; then
            echo "      - ${line}"
            count=$((count + 1))
        fi
    done <<< "${text}"
    local total_lines
    total_lines=$(echo "${text}" | wc -l)
    if [ "${total_lines}" -gt "$max" ]; then
        echo "      ... and $((total_lines - max)) more items"
    fi
}

# ------------------------------------------------------------------------------
# Safety & Containment Guards
# ------------------------------------------------------------------------------
# Validates that a target path is strictly safe to clean.
# Explicitly rejects root, critical system mounts, database directories, DAG files,
# source code, Docker configs, environment files, and JSON datasets.
is_safe_target_path() {
    local path="$1"
    local real_path
    real_path=$(readlink -f "${path}" 2>/dev/null || echo "${path}")

    # Prohibit root or empty paths
    if [[ -z "${real_path}" || "${real_path}" == "/" || "${real_path}" == "/root" || "${real_path}" == "/home" || "${real_path}" == "/etc" || "${real_path}" == "/usr" || "${real_path}" == "/bin" || "${real_path}" == "/sbin" || "${real_path}" == "/lib" || "${real_path}" == "/boot" || "${real_path}" == "/dev" || "${real_path}" == "/sys" || "${real_path}" == "/proc" ]]; then
        return 1
    fi

    # Explicit protection blacklist
    case "${real_path}" in
        */mongodb_data*|*/postgres*|*/redis*)
            return 1
            ;;
        */dags|*/dags/*)
            return 1
            ;;
        */pipeline_metadata*|*/consumer_backup_data*|*/data)
            return 1
            ;;
        *.env*|*airflow_host_key*|*.pem|*.key)
            return 1
            ;;
        *Dockerfile*|*docker-compose*)
            return 1
            ;;
        *.json|*.jsonl)
            return 1
            ;;
        *.py)
            # Never delete source Python files
            return 1
            ;;
    esac

    return 0
}

# ------------------------------------------------------------------------------
# Cleanup Helper Functions
# ------------------------------------------------------------------------------

# Safe file pruning with find
# Usage: prune_files_by_pattern <category_label> <base_dir> <mtime_days> <name_patterns_or_args...>
prune_files_by_pattern() {
    local category="$1"
    local base_dir="$2"
    local mtime_days="$3"
    shift 3

    if [ ! -d "${base_dir}" ]; then
        return 0
    fi

    # Verify base dir safety
    if ! is_safe_target_path "${base_dir}"; then
        echo "[WARNING] Skipped unsafe directory target: ${base_dir}"
        return 0
    fi

    echo "--- [${category}] Scanning: ${base_dir} (older than ${mtime_days} days) ---"

    # Strict exclusions to guarantee protection of critical files
    # Exclude: *.py, *.json, *.jsonl, .env*, mongodb_data, dags, Dockerfile*
    local -a find_cmd=(
        find "${base_dir}" -type f
        -mtime "+${mtime_days}"
        -not -name "*.py"
        -not -name "*.json"
        -not -name "*.jsonl"
        -not -name "*.env"
        -not -name "*.env.*"
        -not -name "Dockerfile*"
        -not -name "docker-compose*"
        -not -path "*/mongodb_data/*"
        -not -path "*/dags/*"
        -not -path "*/.git/*"
    )

    if [ $# -gt 0 ]; then
        find_cmd+=( "$@" )
    fi

    # Measure matching files
    local candidate_list
    candidate_list=$("${find_cmd[@]}" 2>/dev/null || true)

    if [[ -z "${candidate_list}" ]]; then
        echo "    No candidate files found."
        return 0
    fi

    local file_count
    file_count=$(echo "${candidate_list}" | wc -l)

    # Calculate total size in KB
    local total_kb=0
    total_kb=$(echo "${candidate_list}" | tr '\n' '\0' | xargs -0 du -k 2>/dev/null | awk '{sum += $1} END {print sum+0}')

    if [ "${DRY_RUN}" = true ]; then
        echo "    [DRY-RUN] Found ${file_count} candidate file(s), estimated reclaimed: $(format_kb "${total_kb}")"
        print_preview_list "${candidate_list}" 5
        TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + total_kb))
    else
        echo "    Deleting ${file_count} file(s)..."
        echo "${candidate_list}" | tr '\n' '\0' | xargs -0 rm -f 2>/dev/null || true
        echo "    Reclaimed: $(format_kb "${total_kb}")"
        TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + total_kb))
    fi
    PROCESSED_CATEGORIES=$((PROCESSED_CATEGORIES + 1))
}

# Clean Python bytecode (__pycache__ dirs and *.pyc files)
clean_python_bytecode() {
    local target_dir="$1"
    if [ ! -d "${target_dir}" ]; then
        return 0
    fi

    echo "--- [Python Bytecode & Lint Caches] Scanning: ${target_dir} ---"

    # Find __pycache__, .pytest_cache, .ruff_cache, .mypy_cache (exclude virtualenvs, git, database directories)
    local candidate_dirs
    candidate_dirs=$(find "${target_dir}" -depth -type d \( -name "__pycache__" -o -name ".pytest_cache" -o -name ".ruff_cache" -o -name ".mypy_cache" \) \
        -not -path "*/mongodb_data/*" \
        -not -path "*/venv/*" \
        -not -path "*/.venv/*" \
        -not -path "*/env/*" \
        -not -path "*/.git/*" 2>/dev/null || true)

    if [[ -n "${candidate_dirs}" ]]; then
        local dir_count
        dir_count=$(echo "${candidate_dirs}" | wc -l)
        local total_kb=0
        total_kb=$(echo "${candidate_dirs}" | tr '\n' '\0' | xargs -0 du -k -s 2>/dev/null | awk '{sum += $1} END {print sum+0}')

        if [ "${DRY_RUN}" = true ]; then
            echo "    [DRY-RUN] Found ${dir_count} cache directory/directories, estimated: $(format_kb "${total_kb}")"
            print_preview_list "${candidate_dirs}" 5
            TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + total_kb))
        else
            echo "    Removing ${dir_count} bytecode cache directories..."
            echo "${candidate_dirs}" | tr '\n' '\0' | xargs -0 rm -rf 2>/dev/null || true
            echo "    Reclaimed: $(format_kb "${total_kb}")"
            TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + total_kb))
        fi
    else
        echo "    No Python cache directories found."
    fi

    # Find orphaned *.pyc / *.pyo files outside __pycache__
    local candidate_pyc
    candidate_pyc=$(find "${target_dir}" -type f \( -name "*.pyc" -o -name "*.pyo" \) \
        -not -path "*/mongodb_data/*" \
        -not -path "*/venv/*" \
        -not -path "*/.venv/*" \
        -not -path "*/env/*" \
        -not -path "*/.git/*" 2>/dev/null || true)

    if [[ -n "${candidate_pyc}" ]]; then
        local pyc_count
        pyc_count=$(echo "${candidate_pyc}" | wc -l)
        local pyc_kb=0
        pyc_kb=$(echo "${candidate_pyc}" | tr '\n' '\0' | xargs -0 du -k 2>/dev/null | awk '{sum += $1} END {print sum+0}')

        if [ "${DRY_RUN}" = true ]; then
            echo "    [DRY-RUN] Found ${pyc_count} standalone .pyc file(s), estimated: $(format_kb "${pyc_kb}")"
            TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + pyc_kb))
        else
            echo "${candidate_pyc}" | tr '\n' '\0' | xargs -0 rm -f 2>/dev/null || true
            TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + pyc_kb))
        fi
    fi
    PROCESSED_CATEGORIES=$((PROCESSED_CATEGORIES + 1))
}

# Clean IDE and Tool Caches (/root/.cache/JetBrains, ~/.cache/pip)
clean_ide_and_tool_caches() {
    echo "--- [IDE & Tool Caches] Scanning IDE/Pip cache directories ---"
    local raw_cache_dirs=(
        "/root/.cache/JetBrains"
        "${HOME}/.cache/JetBrains"
        "/root/.cache/pip"
        "${HOME}/.cache/pip"
    )

    local -A seen_dirs
    local cache_dirs=()
    for cdir in "${raw_cache_dirs[@]}"; do
        if [ -d "${cdir}" ]; then
            local rdir
            rdir=$(readlink -f "${cdir}" 2>/dev/null || echo "${cdir}")
            if [[ -z "${seen_dirs[${rdir}]:-}" ]]; then
                seen_dirs["${rdir}"]=1
                cache_dirs+=("${rdir}")
            fi
        fi
    done

    for cdir in "${cache_dirs[@]}"; do
        if [[ "${cdir}" == *"JetBrains"* ]]; then
            local jb_kb=0
            jb_kb=$(find "${cdir}" -type f -atime +7 2>/dev/null | tr '\n' '\0' | xargs -0 du -k 2>/dev/null | awk '{sum += $1} END {print sum+0}')
            if [ "${DRY_RUN}" = true ]; then
                echo "    [DRY-RUN] Found stale IDE cache files in ${cdir} (>7d unaccessed): $(format_kb "${jb_kb}")"
                TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + jb_kb))
            else
                find "${cdir}" -type f -atime +7 -delete 2>/dev/null || true
                echo "    Cleaned stale IDE cache in ${cdir}: Reclaimed $(format_kb "${jb_kb}")"
                TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + jb_kb))
            fi
        else
            local pip_kb=0
            pip_kb=$(du -k -s "${cdir}" 2>/dev/null | awk '{print $1+0}')
            if [ "${DRY_RUN}" = true ]; then
                echo "    [DRY-RUN] Found pip cache directory ${cdir}: $(format_kb "${pip_kb}")"
                TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + pip_kb))
            else
                rm -rf "${cdir:?}"/* 2>/dev/null || true
                echo "    Cleaned ${cdir}: Reclaimed $(format_kb "${pip_kb}")"
                TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + pip_kb))
            fi
        fi
    done
    PROCESSED_CATEGORIES=$((PROCESSED_CATEGORIES + 1))
}

# Clean OS Package Manager Caches & Autoremove
clean_package_manager() {
    if command -v apt-get &>/dev/null && [ "$(id -u)" -eq 0 ]; then
        echo "--- [System & Package Manager Caches] Vacuuming APT Archives ---"
        local apt_cache_dir="/var/cache/apt/archives"
        local apt_kb=0
        if [ -d "${apt_cache_dir}" ]; then
            apt_kb=$(du -k -s "${apt_cache_dir}" 2>/dev/null | awk '{print $1+0}')
        fi

        if [ "${DRY_RUN}" = true ]; then
            echo "    [DRY-RUN] APT archive cache: $(format_kb "${apt_kb}") (Command: apt-get clean && apt-get autoremove -y --purge)"
            TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + apt_kb))
        else
            apt-get clean -y 2>/dev/null || true
            apt-get autoremove -y --purge 2>/dev/null || true
            echo "    APT clean & autoremove complete. Estimated freed: $(format_kb "${apt_kb}")"
            TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + apt_kb))
        fi
        PROCESSED_CATEGORIES=$((PROCESSED_CATEGORIES + 1))
    fi
}

# Vacuum Systemd Journal Logs
clean_systemd_journal() {
    local vacuum_time="7d"
    local vacuum_size="200M"

    if [ "${IS_EMERGENCY}" = true ]; then
        vacuum_time="2d"
        vacuum_size="100M"
    fi

    if command -v journalctl &>/dev/null && [ "$(id -u)" -eq 0 ]; then
        echo "--- [Systemd Journal Logs] Vacuuming Journal (time=${vacuum_time}, size=${vacuum_size}) ---"
        if [ "${DRY_RUN}" = true ]; then
            echo "    [DRY-RUN] Command: journalctl --vacuum-time=${vacuum_time} --vacuum-size=${vacuum_size}"
        else
            journalctl --vacuum-time="${vacuum_time}" --vacuum-size="${vacuum_size}" 2>/dev/null || true
            echo "    Journal vacuum complete."
        fi
        PROCESSED_CATEGORIES=$((PROCESSED_CATEGORIES + 1))
    fi
}

# Clean Temp Directories (/tmp, /var/tmp)
clean_system_temp() {
    local temp_retention_days=3
    if [ "${IS_EMERGENCY}" = true ]; then
        temp_retention_days=1
    fi

    echo "--- [System Temp Directories] Checking /tmp and /var/tmp (older than ${temp_retention_days} days) ---"
    for tdir in "/tmp" "/var/tmp"; do
        if [ -d "${tdir}" ]; then
            # Protect sockets, fifos, lock files, critical project extensions, database files, and Junie session directories
            local candidate_tmp
            candidate_tmp=$(find "${tdir}" -mindepth 1 -maxdepth 3 -type f \
                -mtime "+${temp_retention_days}" \
                -not -name "*lock*" \
                -not -name "*.sock" \
                -not -name "*.py" \
                -not -name "*.json" \
                -not -name "*.jsonl" \
                -not -name "*.env" \
                -not -name "*.env.*" \
                -not -name "Dockerfile*" \
                -not -name "docker-compose*" \
                -not -name "*.wt" \
                -not -name "*.db" \
                -not -name "*.sqlite*" \
                -not -path "*/mongodb_data/*" \
                -not -path "*/postgres*" \
                -not -path "*/junie/*" \
                -not -path "*/systemd*" 2>/dev/null || true)

            if [[ -n "${candidate_tmp}" ]]; then
                local tmp_count
                tmp_count=$(echo "${candidate_tmp}" | wc -l)
                local tmp_kb=0
                tmp_kb=$(echo "${candidate_tmp}" | tr '\n' '\0' | xargs -0 du -k 2>/dev/null | awk '{sum += $1} END {print sum+0}')

                if [ "${DRY_RUN}" = true ]; then
                    echo "    [DRY-RUN] Found ${tmp_count} stale temp file(s) in ${tdir}: $(format_kb "${tmp_kb}")"
                    TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + tmp_kb))
                else
                    echo "${candidate_tmp}" | tr '\n' '\0' | xargs -0 rm -f 2>/dev/null || true
                    echo "    Cleaned ${tdir}: ${tmp_count} file(s) removed ($(format_kb "${tmp_kb}"))."
                    TOTAL_RECLAIMED_KB=$((TOTAL_RECLAIMED_KB + tmp_kb))
                fi
            fi
        fi
    done
    PROCESSED_CATEGORIES=$((PROCESSED_CATEGORIES + 1))
}

# ------------------------------------------------------------------------------
# Main Cleanup Execution Flow
# ------------------------------------------------------------------------------

START_TIME=$(date +%s)
START_TIMESTAMP=$(date --iso-8601=seconds 2>/dev/null || date +"%Y-%m-%dT%H:%M:%S%z")

read -r INITIAL_TOTAL_KB INITIAL_USED_KB INITIAL_AVAIL_KB INITIAL_USED_PCT < <(get_disk_metrics "${TARGET_MOUNT}")

IS_EMERGENCY=false
if [ "${FORCE_EMERGENCY}" = true ]; then
    IS_EMERGENCY=true
    CLEANUP_MODE="EMERGENCY (forced)"
elif [ "${INITIAL_USED_PCT}" -ge "${EMERGENCY_USED_PCT_THRESHOLD}" ] || [ "${INITIAL_AVAIL_KB}" -lt "${EMERGENCY_AVAIL_KB_THRESHOLD}" ]; then
    IS_EMERGENCY=true
    CLEANUP_MODE="EMERGENCY (threshold triggered: used=${INITIAL_USED_PCT}%, avail=$(format_kb "${INITIAL_AVAIL_KB}"))"
else
    CLEANUP_MODE="ROUTINE"
fi

echo "=============================================================================="
echo "GSMLS HOST SYSTEM CLEANUP & MAINTENANCE"
echo "=============================================================================="
echo "Timestamp:           ${START_TIMESTAMP}"
echo "Target Mount:        ${TARGET_MOUNT}"
echo "Project Directory:   ${PROJECT_ROOT}"
echo "Execution Mode:      ${CLEANUP_MODE}"
echo "Dry-Run:             ${DRY_RUN}"
echo "Initial Disk Usage:  ${INITIAL_USED_PCT}% (Used: $(format_kb "${INITIAL_USED_KB}") / Total: $(format_kb "${INITIAL_TOTAL_KB}"))"
echo "Initial Available:   $(format_kb "${INITIAL_AVAIL_KB}")"
echo "------------------------------------------------------------------------------"

# 1. System Caches & Package Managers
clean_package_manager

# 2. Systemd Journal Vacuuming
clean_systemd_journal

# 3. System Temporary Directories
clean_system_temp

# 4. IDE & Python Tool Caches
clean_ide_and_tool_caches

# 5. Application & Spark Logs
LOG_RETENTION_DAYS=14
SCHEDULER_LOG_RETENTION_DAYS=7
if [ "${IS_EMERGENCY}" = true ]; then
    LOG_RETENTION_DAYS=3
    SCHEDULER_LOG_RETENTION_DAYS=3
fi

# PySpark execution logs
prune_files_by_pattern "PySpark Logs" "${PROJECT_ROOT}/logs/pyspark_logs" "${LOG_RETENTION_DAYS}"
prune_files_by_pattern "Root PySpark Logs" "${PROJECT_ROOT}/pyspark_logs" "${LOG_RETENTION_DAYS}"

# Airflow Scheduler / DAG processor logs
prune_files_by_pattern "Airflow Scheduler Logs" "${PROJECT_ROOT}/logs/scheduler" "${SCHEDULER_LOG_RETENTION_DAYS}"
prune_files_by_pattern "Airflow DAG Processor Logs" "${PROJECT_ROOT}/logs/dag_processor" "${SCHEDULER_LOG_RETENTION_DAYS}"

# General application logs older than retention period
prune_files_by_pattern "General App Logs" "${PROJECT_ROOT}/logs" "${LOG_RETENTION_DAYS}" \( -name "*.log" -o -name "*.log.*" -o -name "*.out" \)

# 6. Ephemeral Selenium / Scraper Downloads
DOWNLOAD_RETENTION_DAYS=2
if [ "${IS_EMERGENCY}" = true ]; then
    DOWNLOAD_RETENTION_DAYS=1
fi

prune_files_by_pattern "Scraper Downloads" "${PROJECT_ROOT}/downloads" "${DOWNLOAD_RETENTION_DAYS}" \( -name "*.tmp" -o -name "*.crdownload" -o -name "*.xls" -o -name "*.tsv" -o -name "*.csv" \)

# 7. Python Bytecode & Linting Caches across project
clean_python_bytecode "${PROJECT_ROOT}"

# ------------------------------------------------------------------------------
# Final Metrics & Structured Summary
# ------------------------------------------------------------------------------
END_TIME=$(date +%s)
DURATION_SEC=$((END_TIME - START_TIME))
END_TIMESTAMP=$(date --iso-8601=seconds 2>/dev/null || date +"%Y-%m-%dT%H:%M:%S%z")

read -r FINAL_TOTAL_KB FINAL_USED_KB FINAL_AVAIL_KB FINAL_USED_PCT < <(get_disk_metrics "${TARGET_MOUNT}")

if [ "${DRY_RUN}" = true ]; then
    ACTUAL_FREED_KB="${TOTAL_RECLAIMED_KB}"
    NET_CHANGE_KB="${TOTAL_RECLAIMED_KB}"
else
    NET_CHANGE_KB=$((FINAL_AVAIL_KB - INITIAL_AVAIL_KB))
    if [ "${NET_CHANGE_KB}" -lt 0 ]; then
        ACTUAL_FREED_KB=0
    else
        ACTUAL_FREED_KB="${NET_CHANGE_KB}"
    fi
fi

echo "=============================================================================="
echo "CLEANUP EXECUTION SUMMARY"
echo "=============================================================================="
echo "Status:              SUCCESS"
echo "Duration:            ${DURATION_SEC}s"
echo "Mode:                ${CLEANUP_MODE}"
echo "Dry-Run:             ${DRY_RUN}"
echo "Pre-Cleanup Space:   $(format_kb "${INITIAL_AVAIL_KB}") available (${INITIAL_USED_PCT}% used)"
echo "Post-Cleanup Space:  $(format_kb "${FINAL_AVAIL_KB}") available (${FINAL_USED_PCT}% used)"
echo "Estimated Reclaimed: $(format_kb "${TOTAL_RECLAIMED_KB}")"
echo "=============================================================================="

# Machine-readable JSON summary for Airflow XCom capture and monitoring
cat <<EOF
{"timestamp":"${END_TIMESTAMP}","status":"SUCCESS","duration_sec":${DURATION_SEC},"dry_run":${DRY_RUN},"is_emergency":${IS_EMERGENCY},"initial_available_kb":${INITIAL_AVAIL_KB},"initial_used_pct":${INITIAL_USED_PCT},"final_available_kb":${FINAL_AVAIL_KB},"final_used_pct":${FINAL_USED_PCT},"reclaimed_kb":${TOTAL_RECLAIMED_KB},"categories_scanned":${PROCESSED_CATEGORIES}}
EOF

exit 0
