#!/bin/bash
# ==============================================================================
# Hudi MOR表异步Compaction自动化执行脚本
# 功能: Kerberos认证 + 配置查询 + Compaction执行 + Application ID捕获
# 依赖: lib/{auth,log,utils}.sh
# 版本: 3.0 | 更新: 2025-08-28
# ==============================================================================

set -euo pipefail  # 严格错误处理

# 日志级别初始化
export LOG_LEVEL="${LOG_LEVEL:-INFO}"

# 模块加载
readonly SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
for lib in utils log auth; do
    source "${SCRIPT_DIR}/lib/${lib}.sh" || exit 1
done

# 全局配置
readonly MRS_HOME="/opt/mrs/client"
readonly KEYTAB_PATH="/keytabs/dlk_shark_dev.keytab"
readonly PRINCIPAL="dlk_shark_dev@EXAMPLE.COM"
readonly OUTPUT_BASE_DIR="${HOME}/tmp/hudi_compaction"
readonly CONFIG_TABLE="hudi_mor_compaction_config"
readonly CONFIG_DB="dlk_hdm_dev"

# 查询超时配置（秒）
readonly QUERY_TIMEOUT="${QUERY_TIMEOUT:-300}"  # 默认5分钟，可通过环境变量覆盖

# 查询生效配置的SQL语句
readonly CONFIG_QUERY="
SELECT 
    full_table_name,
    compaction_commit_threshold,
    spark_executor_cores,
    spark_executor_memory,
    spark_driver_memory,
    spark_shuffle_partitions,
    spark_dynamic_allocation,
    spark_dynamic_min_executors,
    spark_dynamic_max_executors,
    spark_dynamic_initial_executors,
    schedule_hours,
    schedule_timezone,
    priority_level,
    enable_monitoring,
    alert_on_failure,
    alert_recipients,
    max_retry_times,
    remark,
    load_dt
FROM ${CONFIG_DB}.v_active_hudi_mor_compaction_config 
ORDER BY priority_level ASC;"

# 查询配置参数并返回结果文件路径
query_compaction_configs() {
    local timestamp=$(date +%s)
    local output_dir="${OUTPUT_BASE_DIR}/config"
    local error_dir="${OUTPUT_BASE_DIR}/error"
    local config_file="${output_dir}/config_${timestamp}.csv"
    local error_log="${error_dir}/config_error_${timestamp}.log"
    
    # 创建目录
    safe_mkdir "$output_dir" 750 || return 1
    safe_mkdir "$error_dir" 750 || return 1
    
    log "INFO" "查询Compaction配置参数..."
    
    # Spark配置
    local -a cmd=(
        spark-sql --master yarn --name "Hudi_Config_Query_${timestamp}"
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.dynamicAllocation.minExecutors=1
        --conf spark.dynamicAllocation.maxExecutors=5
        --conf spark.dynamicAllocation.initialExecutors=1
        --conf spark.shuffle.service.enabled=true
        --conf spark.executor.cores=1
        --conf spark.executor.memory=1g
        --conf spark.driver.memory=1g
        --conf spark.sql.shuffle.partitions=5
        --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
        -e "$CONFIG_QUERY"
    )
    
    log "INFO" "执行配置查询任务..."
    
    # 执行查询（带进度提示和超时控制）
    log "INFO" "正在执行配置查询，请稍候..."
    log "INFO" "查询命令: ${cmd[0]} ... (完整参数请查看日志)"
    
    # 启动后台查询进程
    "${cmd[@]}" >"$config_file" 2>"$error_log" &
    local query_pid=$!
    
    # 设置查询超时时间（秒）
    local timeout=${QUERY_TIMEOUT:-300}  # 默认5分钟超时
    local elapsed=0
    local progress_interval=10  # 每10秒显示一次进度
    
    # 进度监控循环
    while kill -0 "$query_pid" 2>/dev/null; do
        sleep $progress_interval
        elapsed=$((elapsed + progress_interval))
        
        # 显示进度信息
        log "INFO" "查询进行中... 已用时: ${elapsed}秒"
        
        # 检查超时
        if [[ $elapsed -ge $timeout ]]; then
            log "WARN" "查询超时（${timeout}秒），正在终止查询进程..."
            kill -TERM "$query_pid" 2>/dev/null
            sleep 5
            if kill -0 "$query_pid" 2>/dev/null; then
                log "WARN" "强制终止查询进程"
                kill -KILL "$query_pid" 2>/dev/null
            fi
            log "ERROR" "配置查询超时失败，请检查数据库连接或查询条件"
            return 1
        fi
    done
    
    # 等待进程完成并获取退出码
    wait "$query_pid"
    local query_exit_code=$?
    
    log "INFO" "查询执行完成，总用时: ${elapsed}秒"
    
    # 检查查询结果
    if [[ $query_exit_code -eq 0 ]]; then
        if [[ -s "$config_file" ]]; then
            # 验证文件内容：检查是否包含有效数据行（至少有标题行+1行数据）
            local line_count=$(wc -l < "$config_file")
            if [[ $line_count -gt 1 ]]; then
                # 预览查询结果
                log "INFO" "配置查询成功，共查询到 $((line_count - 1)) 条配置记录"
                log "INFO" "结果文件: $config_file"
                log "INFO" "结果预览（前3行）:"
                head -3 "$config_file" | while read -r line; do 
                    log "INFO" "  $line" >&2
                done
                echo "$config_file"
                return 0
            else
                log "WARN" "配置查询结果仅包含标题行，未找到有效的配置数据"
                log "WARN" "请检查配置表中是否存在符合条件的记录"
                return 1
            fi
        else
            log "WARN" "配置查询结果文件为空"
            return 1
        fi
    else
        log "ERROR" "配置查询失败（退出码: $query_exit_code）"
        [[ -s "$error_log" ]] && {
            log "ERROR" "错误信息:"
            tail -3 "$error_log" | while read -r line; do 
                log "ERROR" "  $line" >&2
            done
        }
        return 1
    fi
}

# 解析配置文件并提取配置参数
parse_config_results() {
    local config_file="$1"
    local temp_configs="${OUTPUT_BASE_DIR}/temp_configs.txt"
    
    [[ ! -s "$config_file" ]] && { log "ERROR" "配置文件为空: $config_file"; return 1; }
    
    log "INFO" "解析配置参数..."
    
    # 清空临时配置文件
    > "$temp_configs"
    
    # 跳过标题行，解析数据行（避免管道影响日志输出）
    local line_num=0
    while IFS=$'\t' read -r full_table_name compaction_commit_threshold spark_executor_cores spark_executor_memory spark_driver_memory spark_shuffle_partitions spark_dynamic_allocation spark_dynamic_min_executors spark_dynamic_max_executors spark_dynamic_initial_executors schedule_hours schedule_timezone priority_level enable_monitoring alert_on_failure alert_recipients max_retry_times remark load_dt; do
        ((line_num++))
        
        # 跳过标题行
        [[ $line_num -eq 1 ]] && continue
        
        # 从 full_table_name 中提取 schema_name 和 table_name
        local schema_name table_name
        if [[ "$full_table_name" =~ ^([^.]+)\.(.+)$ ]]; then
            schema_name="${BASH_REMATCH[1]}"
            table_name="${BASH_REMATCH[2]}"
        else
            log "WARN" "无效的表名格式: $full_table_name" >&2
            continue
        fi
        
        # 验证必需字段
        if [[ -n "$full_table_name" && -n "$compaction_commit_threshold" ]]; then
            # 保存配置到临时文件（保持与原有格式兼容）
            echo "$schema_name|$table_name|$full_table_name|$compaction_commit_threshold|$spark_executor_cores|$spark_executor_memory|$spark_driver_memory|$spark_dynamic_allocation|$spark_dynamic_min_executors|$spark_dynamic_max_executors|$spark_dynamic_initial_executors|$spark_shuffle_partitions" >> "$temp_configs"
            log "INFO" "解析配置: ${full_table_name} (commit_threshold=${compaction_commit_threshold})"
        else
            log "WARN" "跳过无效配置行: full_table_name=${full_table_name}, commit_threshold=${compaction_commit_threshold}" >&2
        fi
    done < "$config_file"
    
    if [[ -s "$temp_configs" ]]; then
        echo "$temp_configs"
        return 0
    else
        log "ERROR" "未找到有效的配置参数"
        return 1
    fi
}

# 生成并执行Compaction SQL语句
generate_and_execute_compaction() {
    local schema_name="$1"
    local table_name="$2"
    local full_table_name="$3"
    local compaction_commit_threshold="$4"
    local spark_executor_cores="$5"
    local spark_executor_memory="$6"
    local spark_driver_memory="$7"
    local spark_dynamic_allocation="$8"
    local spark_dynamic_min_executors="$9"
    local spark_dynamic_max_executors="${10}"
    local spark_dynamic_initial_executors="${11}"
    local spark_shuffle_partitions="${12}"
    
    local timestamp=$(date +%s)
    local output_dir="${OUTPUT_BASE_DIR}/compaction"
    local error_dir="${OUTPUT_BASE_DIR}/error"
    
    # 从 full_table_name 中提取 table_name 用于文件命名
    local extracted_table_name
    if [[ "$full_table_name" =~ ^[^.]+\.(.+)$ ]]; then
        extracted_table_name="${BASH_REMATCH[1]}"
    else
        extracted_table_name="${table_name}"
    fi
    
    local compaction_log="${output_dir}/compaction_${extracted_table_name}_${timestamp}.log"
    local error_log="${error_dir}/compaction_error_${extracted_table_name}_${timestamp}.log"
    
    # 创建目录
    safe_mkdir "$output_dir" 750 || return 1
    safe_mkdir "$error_dir" 750 || return 1
    
    # 计算清理和归档参数
    local cleaner_commits_retained=$((compaction_commit_threshold * 2))
    local keep_min_commits=$((cleaner_commits_retained + 1))
    local keep_max_commits=$((keep_min_commits + 20))
    
    log "INFO" "开始执行Compaction: ${full_table_name}"
    log "INFO" "参数计算 - cleaner_commits_retained=${cleaner_commits_retained}, keep_min_commits=${keep_min_commits}, keep_max_commits=${keep_max_commits}"
    
    # 生成Compaction SQL语句
    local compaction_sql="
set hoodie.compact.inline = true;
set hoodie.run.compact.only.inline = true;
set hoodie.cleaner.commits.retained = ${cleaner_commits_retained};
set hoodie.keep.max.commits = ${keep_max_commits};
set hoodie.keep.min.commits = ${keep_min_commits};
set hoodie.clean.async = false;
set hoodie.clean.automatic = false;
set hoodie.archive.async = false;
set hoodie.archive.automatic = false;

run compaction on ${full_table_name};
run clean on ${full_table_name};
run archivelog on ${full_table_name};"
    
    # Spark配置（使用从配置表获取的参数）
    local dynamic_allocation_enabled="false"
    [[ "$spark_dynamic_allocation" == "true" ]] && dynamic_allocation_enabled="true"
    
    local -a cmd=(
        spark-sql --master yarn --name "Hudi_Compaction_${extracted_table_name}_${timestamp}"
        --conf spark.dynamicAllocation.enabled=${dynamic_allocation_enabled}
    )
    
    # 根据动态分配设置添加不同的配置
    if [[ "$dynamic_allocation_enabled" == "true" ]]; then
        cmd+=(
            --conf spark.dynamicAllocation.minExecutors=${spark_dynamic_min_executors}
            --conf spark.dynamicAllocation.maxExecutors=${spark_dynamic_max_executors}
            --conf spark.dynamicAllocation.initialExecutors=${spark_dynamic_initial_executors}
        )
    else
        cmd+=(--conf spark.executor.instances=2)
    fi
    
    cmd+=(
        --conf spark.executor.cores=${spark_executor_cores}
        --conf spark.executor.memory=${spark_executor_memory}
        --conf spark.driver.memory=${spark_driver_memory}
        --conf spark.sql.shuffle.partitions=${spark_shuffle_partitions}
        --conf spark.shuffle.service.enabled=true
        --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
        -e "$compaction_sql"
    )
    
    log "INFO" "提交Compaction任务: ${cmd[0]} ... (参数略)"
    
    # 执行Compaction并捕获Application ID
    local app_id=""
    if "${cmd[@]}" >"$compaction_log" 2>"$error_log"; then
        app_id=$(grep -m1 -o 'application_[0-9]\{13\}_[0-9]\{4\}' "$compaction_log" "$error_log" 2>/dev/null | head -1)
        
        if [[ "$app_id" =~ ^application_[0-9]{13}_[0-9]{4}$ ]]; then
            log "INFO" "Compaction执行成功 - ${full_table_name} (Application ID: $app_id)"
            # 更新配置表状态
            update_compaction_status "$schema_name" "$table_name" "SUCCESS" "$app_id"
            return 0
        else
            log "WARN" "Compaction可能成功但未获取到有效Application ID - ${full_table_name}"
            update_compaction_status "$schema_name" "$table_name" "SUCCESS" ""
            return 0
        fi
    else
        log "ERROR" "Compaction执行失败 - ${full_table_name}"
        
        # 错误处理
        [[ -s "$error_log" ]] && {
            log "ERROR" "错误信息:"
            tail -5 "$error_log" | while read -r line; do 
                log "ERROR" "  $line" >&2
            done
        }
        
        # 更新失败状态
        update_compaction_status "$schema_name" "$table_name" "FAILED" ""
        return 1
    fi
}

# 更新Compaction执行状态到配置表
update_compaction_status() {
    local schema_name="$1"
    local table_name="$2"
    local status="$3"
    local app_id="$4"
    
    local timestamp=$(date +%s)
    local update_sql="
UPDATE ${CONFIG_DB}.${CONFIG_TABLE} 
SET last_compaction_time = CURRENT_TIMESTAMP,
    last_compaction_status = '${status}',
    yarn_application_id = '${app_id}',
    updated_time = CURRENT_TIMESTAMP,
    updated_by = 'SYSTEM_AUTO'
WHERE schema_name = '${schema_name}' AND table_name = '${table_name}';"
    
    local -a cmd=(
        spark-sql --master yarn --name "Hudi_Status_Update_${timestamp}"
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.dynamicAllocation.minExecutors=1
        --conf spark.dynamicAllocation.maxExecutors=2
        --conf spark.dynamicAllocation.initialExecutors=1
        --conf spark.shuffle.service.enabled=true
        --conf spark.executor.cores=1
        --conf spark.executor.memory=512m
        --conf spark.driver.memory=512m
        --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
        -e "$update_sql"
    )
    
    log "INFO" "更新Compaction状态: ${schema_name}.${table_name} -> ${status}"
    
    if "${cmd[@]}" >/dev/null 2>&1; then
        log "INFO" "状态更新成功"
    else
        log "WARN" "状态更新失败，但不影响主流程"
    fi
}

# 主控制流程
main() {
    log "INFO" "===== Hudi MOR表异步Compaction任务启动 ====="
    
    # 初始化输出基础目录
    log "INFO" "初始化输出目录: $OUTPUT_BASE_DIR"
    if safe_mkdir "$OUTPUT_BASE_DIR" 755; then
        log "INFO" "[SUCCESS] 输出目录创建成功: $OUTPUT_BASE_DIR"
    else
        log "ERROR" "[FAILED] 输出目录创建失败: $OUTPUT_BASE_DIR"
        exit 1
    fi
    
    # 环境初始化和认证
    init_mrs_environment "$MRS_HOME" || exit 1
    log "INFO" "执行Kerberos认证"
    local krb_cache
    krb_cache=$(kerberos_auth "$KEYTAB_PATH" "$PRINCIPAL") || {
        log "ERROR" "Kerberos认证失败，任务终止"; exit 1
    }
    
    # 资源清理钩子
    trap 'cleanup_kerberos "$krb_cache"; rotate_logs "$OUTPUT_BASE_DIR" 7; log "INFO" "资源已清理"' EXIT
    
    # 第一步：查询配置参数
    log "INFO" "=== 步骤1: 查询Compaction配置参数 ==="
    local config_file
    config_file=$(query_compaction_configs) || {
        log "ERROR" "配置查询失败，任务终止"; exit 1
    }
    
    # 第二步：解析配置参数
    log "INFO" "=== 步骤2: 解析配置参数 ==="
    local temp_configs
    temp_configs=$(parse_config_results "$config_file") || {
        log "ERROR" "配置解析失败，任务终止"; exit 1
    }
    
    # 第三步：逐个执行Compaction
    log "INFO" "=== 步骤3: 执行Compaction操作 ==="
    local success_count=0
    local failed_count=0
    local total_count=0
    
    while IFS='|' read -r schema_name table_name full_table_name compaction_commit_threshold spark_executor_cores spark_executor_memory spark_driver_memory spark_dynamic_allocation spark_dynamic_min_executors spark_dynamic_max_executors spark_dynamic_initial_executors spark_shuffle_partitions; do
        ((total_count++))
        
        log "INFO" "处理表 $total_count: ${full_table_name}"
        
        if generate_and_execute_compaction "$schema_name" "$table_name" "$full_table_name" "$compaction_commit_threshold" "$spark_executor_cores" "$spark_executor_memory" "$spark_driver_memory" "$spark_dynamic_allocation" "$spark_dynamic_min_executors" "$spark_dynamic_max_executors" "$spark_dynamic_initial_executors" "$spark_shuffle_partitions"; then
            ((success_count++))
            log "INFO" "[SUCCESS] ${full_table_name} Compaction执行成功"
        else
            ((failed_count++))
            log "ERROR" "[FAILED] ${full_table_name} Compaction执行失败"
        fi
        
        # 添加间隔，避免过于频繁的提交
        sleep 5
        
    done < "$temp_configs"
    
    # 第四步：任务总结
    log "INFO" "=== 步骤4: 任务执行总结 ==="
    log "INFO" "总计处理: $total_count 个表"
    log "INFO" "成功执行: $success_count 个表"
    log "INFO" "执行失败: $failed_count 个表"
    
    if [[ $failed_count -eq 0 ]]; then
        log "INFO" "[COMPLETE] 所有Compaction任务均执行成功！"
        exit 0
    else
        log "WARN" "[WARNING] 部分Compaction任务执行失败，请检查错误日志"
        exit 1
    fi
}

# 脚本入口
main "$@"