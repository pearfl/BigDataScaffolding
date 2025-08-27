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
    local env_files=("${mrs_home}/bigdata_env" "${mrs_home}/Hudi/component_env")
    local loaded_count=0
    
    for env_file in "${env_files[@]}"; do
        [[ ! -f "$env_file" ]] && { log "ERROR" "环境文件缺失: $env_file"; return 1; }
        
        log "DEBUG" "加载环境配置: $env_file"
        if source "$env_file" 2>/dev/null; then
            ((loaded_count++))
            log "DEBUG" "环境文件加载成功: $env_file"
        else
            log "ERROR" "环境加载失败: $env_file"; return 1
        fi
    done
    
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