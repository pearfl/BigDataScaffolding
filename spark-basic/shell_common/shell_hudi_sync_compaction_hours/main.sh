#!/bin/bash
# ==============================================================================
# Spark SQL自动化查询脚本
# 功能: Kerberos认证 + Spark查询 + Application ID捕获
# 依赖: lib/{auth,log,utils}.sh
# 版本: 2.1 | 更新: 2025-08-27
# ==============================================================================

set -euo pipefail  # 严格错误处理

# 模块加载
readonly SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
for lib in utils log auth; do
    source "${SCRIPT_DIR}/lib/${lib}.sh" || exit 1
done

# 全局配置
readonly MRS_HOME="/opt/mrs/client"
readonly KEYTAB_PATH="/keytabs/dlk_shark_dev.keytab"
readonly PRINCIPAL="dlk_shark_dev@EXAMPLE.COM"
readonly OUTPUT_BASE_DIR="/home/deploy/tmp/sparksql"
readonly SQL_QUERY="SELECT * FROM dlk_sor_dev.rtsor_log_ccdlk_openserver_mor;"

# Spark SQL执行函数 - 提交查询并捕获Application ID
execute_spark_query() {
    local timestamp=$(date +%s)
    local output_dir="${OUTPUT_BASE_DIR}/result"
    local error_dir="${OUTPUT_BASE_DIR}/error"
    local spark_log="${output_dir}/spark_${timestamp}.log"
    local error_log="${error_dir}/error_${timestamp}.log"
    
    # 创建目录
    safe_mkdir "$output_dir" 750 || return 1
    safe_mkdir "$error_dir" 750 || return 1
    
    # Spark配置
    local -a cmd=(
        spark-sql --master yarn --name "Hudi_Query_${timestamp}"
        --conf spark.dynamicAllocation.enabled=false
        --conf spark.executor.instances=2 --conf spark.executor.cores=1 
        --conf spark.executor.memory=1g --conf spark.driver.memory=1g
        --conf spark.sql.shuffle.partitions=10
        --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
        -e "$SQL_QUERY"
    )
    
    log "INFO" "提交Spark任务: ${cmd[0]} ... (参数略)"
    
    # 执行并捕获Application ID
    local app_id=""
    if "${cmd[@]}" >"$spark_log" 2>"$error_log"; then
        app_id=$(grep -m1 -o 'application_[0-9]\{13\}_[0-9]\{4\}' "$spark_log" "$error_log" 2>/dev/null | head -1)
        
        if [[ "$app_id" =~ ^application_[0-9]{13}_[0-9]{4}$ ]]; then
            log "INFO" "Application ID: $app_id"
            echo "$app_id"; return 0
        fi
        log "WARN" "未获取到有效Application ID"
    else
        log "ERROR" "Spark任务执行失败"
    fi
    
    # 错误处理
    [[ -s "$error_log" ]] && {
        log "ERROR" "错误信息:"; tail -3 "$error_log" | while read -r line; do log "ERROR" "  $line"; done
    }
    return 1
}

# 结果处理函数 - 验证并预览查询结果
process_query_results() {
    local output_file="$1"
    [[ ! -s "$output_file" ]] && { log "ERROR" "结果文件为空: $output_file"; return 1; }
    
    log "INFO" "=== 结果预览(前5行) ==="
    head -6 "$output_file" | while read -r line; do log "DATA" "$line"; done
}

# 主控制流程
main() {
    log "INFO" "===== Spark SQL查询任务启动 ====="
    
    # 环境初始化和认证
    init_mrs_environment "$MRS_HOME" || exit 1
    log "INFO" "执行Kerberos认证"
    local krb_cache
    krb_cache=$(kerberos_auth "$KEYTAB_PATH" "$PRINCIPAL") || {
        log "ERROR" "Kerberos认证失败，任务终止"; exit 1
    }
    
    # 资源清理钩子
    trap 'cleanup_kerberos "$krb_cache"; rotate_logs "$OUTPUT_BASE_DIR" 1; log "INFO" "资源已清理"' EXIT
    
    # 执行查询
    local app_id
    app_id=$(execute_spark_query) || { log "ERROR" "Spark查询执行失败"; exit 1; }
    
    # 处理结果  
    local result_file="${OUTPUT_BASE_DIR}/result/result_$(date +%s).csv"
    process_query_results "$result_file" || { log "ERROR" "结果处理异常"; exit 1; }
    
    log "INFO" "任务完成 (APP_ID:$app_id)"
}

# 脚本入口
main "$@"