#!/bin/bash
# ==============================================================================
# 日志管理库 - 智能路径/多级过滤/彩色输出/自动轮转
# 版本: 2.1 | 更新: 2025-08-27
# ==============================================================================

readonly LOG_ROOT="${HOME}/tmp/logs"
declare -A LOG_LEVELS=([DEBUG]=4 [INFO]=3 [WARN]=2 [ERROR]=1)

# 日志记录主函数 - 支持级别过滤和彩色输出
log() {
    local level="${1^^}" message="$2"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S.%3N" 2>/dev/null || date "+%Y-%m-%d %H:%M:%S")
   
    # 智能路径生成（三级兜底）
    local resolved_log_file="${3:-${TARGET_LOG_FILE:-}}"
    if [[ -z "$resolved_log_file" ]]; then
        local date_dir="${LOG_ROOT}/$(date +%Y%m%d)"
        mkdir -p "$date_dir" 2>/dev/null || {
            echo "ERROR: 无法创建日志目录 [$date_dir]" >&2; return 1
        }
        resolved_log_file="${date_dir}/log_$(date +%H%M%S)_${RANDOM}.log"
        export TARGET_LOG_FILE="$resolved_log_file"
        echo "NOTICE: 自动日志路径 [$resolved_log_file]" >&2
    fi
   
    # 日志级别过滤
    [[ "${LOG_LEVELS[$level]}" -le "${LOG_LEVELS[${LOG_LEVEL:-INFO}]}" ]] || return 0
   
    # 终端颜色配置
    local color_reset="\033[0m" color_start="" level_tag=""
    case "$level" in
        ERROR)
            color_start="\033[1;31m"  # 红色粗体
            level_tag="[ERROR]"
            ;;
        WARN)  
            color_start="\033[1;33m"  # 黄色粗体
            level_tag="[WARN]"
            ;;
        INFO)  
            color_start="\033[1;32m"  # 绿色粗体
            level_tag="[INFO]"
            ;;
        DEBUG)
            color_start="\033[1;34m"  # 蓝色粗体
            level_tag="[DEBUG]"
            ;;
        *)
            level_tag="[$level]"
            ;;
    esac
   
    # 生成日志条目（分别处理终端和文件格式）
    local base_entry="${timestamp} ${level_tag} ${message}"
    local console_entry="${timestamp} ${color_start}${level_tag}${color_reset} ${message}"
   
    # 控制台输出（ERROR -> stderr, 其他 -> stdout）
    if [[ "$level" == "ERROR" ]]; then
        echo -e "$console_entry" >&2
    else
        echo -e "$console_entry"
    fi
   
    # 文件写入（使用统一格式）
    if [[ -n "$resolved_log_file" ]]; then
        mkdir -p "$(dirname "$resolved_log_file")" 2>/dev/null
        # 使用exec重定向确保写入完整性，避免缓冲问题
        exec 3>> "$resolved_log_file"
        printf '%s\n' "$base_entry" >&3
        exec 3>&-
    fi
}

# 日志轮转函数 - 清理过期文件和空目录
rotate_logs() {
    local log_root="${1:-$LOG_ROOT}" retain_days="${2:-7}"
   
    [[ ! -d "$log_root" ]] && { log "ERROR" "目录不存在: $log_root"; return 1; }
   
    # 清理过期日志和空目录
    find "$log_root" -type f -name "*.log" -mtime +"$retain_days" -delete 2>/dev/null
    find "$log_root" -type d -empty -delete 2>/dev/null
   
    log "INFO" "已清理[$log_root]中超过${retain_days}天的日志"
}