#!/bin/bash
# ==============================================================================
# 路径处理工具库 - 标准化/安全创建/注入防护
# 版本: 2.1 | 更新: 2025-08-27
# ==============================================================================

# 路径标准化 - 转为绝对路径并解析符号链接
normalize_path() {
    local path="$1"
    [[ -z "$path" ]] && { echo "错误：路径参数为空" >&2; return 1; }
    
    # 绝对路径直接处理
    if [[ "$path" = /* ]]; then
        realpath -m "$path" 2>/dev/null && return 0
    fi
    
    # 相对路径转换
    local parent_dir=$(dirname "$path")
    local file_name=$(basename "$path")
    
    cd "$parent_dir" 2>/dev/null || {
        echo "错误：父目录不存在 [$parent_dir]" >&2; return 2
    }
    
    echo "$(pwd)/$file_name"
}

# 安全目录创建 - 递归创建并设置严格权限
safe_mkdir() {
    local raw_dir="$1" perm="${2:-0755}"
    local target_dir
    target_dir=$(normalize_path "$raw_dir") || return 1
    
    # 目录存在性检查和权限修正
    if [[ -d "$target_dir" ]]; then
        [[ $(stat -c "%a" "$target_dir") != "$perm" ]] && chmod "$perm" "$target_dir"
        return 0
    fi
    
    # 安全创建目录
    local old_umask=$(umask); umask 0077
    mkdir -p "$target_dir" 2>/dev/null || {
        umask "$old_umask"
        echo "目录创建失败: $target_dir" >&2; return 2
    }
    umask "$old_umask"
    
    # 设置权限
    chmod "$perm" "$target_dir" || {
        echo "权限设置失败: $target_dir (目标权限:$perm)" >&2; return 3
    }
}

# 路径安全验证 - 防御路径遍历攻击和越权访问
validate_path_safe() {
    local path="$1" allowed_root="$2"
    
    # 注入攻击特征检测
    [[ "$path" =~ (\.\./|/\.\.|//|/\./) ]] && {
        echo "安全告警：路径包含非法序列 [$path]" >&2; return 1
    }
    
    # 标准化路径以验证边界
    local resolved_path
    resolved_path=$(normalize_path "$path") || return 2
    
    # 根目录限制检查
    if [[ -n "$allowed_root" ]]; then
        local resolved_root
        resolved_root=$(normalize_path "$allowed_root") || return 3
        
        [[ ! "$resolved_path" == "$resolved_root"* ]] && {
            echo "安全告警：路径超出允许范围 [$resolved_path]" >&2; return 4
        }
    fi
}