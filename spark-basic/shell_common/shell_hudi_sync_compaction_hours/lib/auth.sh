#!/bin/bash
# ==============================================================================
# Kerberos认证与MRS环境管理模块
# 功能: Kerberos认证生命周期管理 + MRS环境初始化
# 版本: 2.1 | 更新: 2025-08-27
# ==============================================================================

# MRS环境初始化 - 加载Hadoop/Spark环境配置
init_mrs_environment() {
    local mrs_home="${1:-$MRS_HOME}"
    log "INFO" "开始初始化MRS客户端环境 [${mrs_home}]"
    
    # 参数验证
    [[ -z "$mrs_home" ]] && {
        log "ERROR" "MRS主目录未指定，请设置MRS_HOME环境变量或传入参数"; return 1
    }
    
    # 安全和存在性验证
    validate_path_safe "$mrs_home" || { log "ERROR" "MRS路径不安全: $mrs_home"; return 1; }
    [[ ! -d "$mrs_home" ]] && { log "ERROR" "MRS客户端目录不存在: $mrs_home"; return 1; }

    # 环境文件加载
    local env_files=("${mrs_home}/JDK/component_env" "${mrs_home}/KrbClient/component_env" "${mrs_home}/Hudi/component_env")
    local loaded_count=0
    
    for env_file in "${env_files[@]}"; do
        # 文件存在性检查
        if [[ ! -f "$env_file" ]]; then
            log "ERROR" "环境文件不存在: $env_file"
            log "ERROR" "请检查MRS客户端安装目录结构"
            return 1
        fi
        
        # 文件权限检查
        if [[ ! -r "$env_file" ]]; then
            log "ERROR" "环境文件无读取权限: $env_file"
            return 1
        fi
        
        log "DEBUG" "开始加载环境配置: $env_file"
        log "DEBUG" "文件大小: $(stat -c%s "$env_file" 2>/dev/null || echo "未知") 字节"
        
        # 先检查文件内容是否包含有害命令
        if grep -q -E '^\s*(exit|return)' "$env_file" 2>/dev/null; then
            log "WARN" "环境文件包含exit/return命令，可能会导致脚本退出: $env_file"
        fi
        
        # 保存当前设置状态
        local old_set_state=$(set +o | grep -E '(errexit|nounset|pipefail)')
        
        # 使用子shell来隔离加载过程，防止影响主脚本
        if (
            # 在子shell中临时关闭严格模式
            set +euo pipefail
            log "DEBUG" "在子shell中加载: $env_file"
            source "$env_file"
        ) 2>/dev/null; then
            # 恢复原有设置
            eval "$old_set_state"
            
            # 在主脚本中重新加载环境变量
            if source "$env_file" 2>/dev/null; then
                ((loaded_count++))
                log "DEBUG" "环境文件加载成功: $env_file"
                
                # 验证关键环境变量
                if [[ -n "${HADOOP_HOME:-}" ]]; then
                    log "DEBUG" "Hadoop环境检测到: HADOOP_HOME=${HADOOP_HOME}"
                fi
                if [[ -n "${SPARK_HOME:-}" ]]; then
                    log "DEBUG" "Spark环境检测到: SPARK_HOME=${SPARK_HOME}"
                fi
            else
                log "ERROR" "环境文件在主脚本中加载失败: $env_file"
                return 1
            fi
        else
            # 恢复原有设置
            eval "$old_set_state"
            
            log "ERROR" "环境文件在子shell中加载失败: $env_file"
            log "ERROR" "请检查文件语法或权限问题"
            
            # 显示文件内容的前几行用于调试
            log "DEBUG" "文件前5行内容:"
            head -5 "$env_file" 2>/dev/null | while read -r line; do
                log "DEBUG" "  $line" >&2
            done
            
            return 1
        fi
    done
    
    # 导出JAAS配置文件路径
    if [[ -n "${KRB5_HOME:-}" ]]; then
        export JAAS_CONF="$KRB5_HOME/var/krb5kdc/jaas.conf"
        log "DEBUG" "JAAS配置设置: JAAS_CONF=${JAAS_CONF}"
        
        # 验证JAAS配置文件是否存在
        if [[ -f "$JAAS_CONF" ]]; then
            log "DEBUG" "JAAS配置文件存在: $JAAS_CONF"
        else
            log "WARN" "JAAS配置文件不存在: $JAAS_CONF"
        fi
    else
        log "WARN" "KRB5_HOME环境变量未设置，无法设置JAAS_CONF"
    fi
    
    log "INFO" "MRS客户端环境初始化完成 (已加载${loaded_count}个配置文件)"
}

# Kerberos认证主函数 - TGT获取与缓存管理
kerberos_auth() {
    local keytab_path=$(normalize_path "$1")
    local principal="$2"
    local cache_dir="${3:-${KRB_TMP_DIR:-${HOME}/tmp}}"

    # 密钥文件验证
    [[ ! -f "$keytab_path" ]] && { log "ERROR" "密钥文件不存在: $keytab_path"; return 1; }

    # 创建临时凭证缓存
    local tmp_ccache
    tmp_ccache=$(mktemp "${cache_dir}/krb5cc_${principal}_XXXXXXXXXX") || {
        log "ERROR" "临时凭证缓存创建失败"; return 1
    }
    chmod 600 "$tmp_ccache"

    # 设置环境变量并执行认证
    export KRB5CCNAME="FILE:$tmp_ccache"
    kinit -kt "$keytab_path" "$principal" || {
        log "ERROR" "Kerberos认证失败 (主体:${principal})"
        rm -f "$tmp_ccache"; return 1
    }

    # 票据验证
    klist | grep -q "$principal" || log "WARN" "票据验证异常"
    echo "$tmp_ccache"
}

# Kerberos资源清理函数 - 安全销毁凭证
cleanup_kerberos() {
    local ccache="$1"
    kdestroy -c "$ccache" 2>/dev/null
    rm -f "$ccache"
    log "DEBUG" "Kerberos凭证已清理"
}