#!/bin/bash
# Docker 容器健康监控脚本

set -euo pipefail

# 配置
PROJECT_NAME="codex-proxy"
ALERT_EMAIL="${ALERT_EMAIL:-}"
WEBHOOK_URL="${WEBHOOK_URL:-}"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] ℹ️  $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ❌ $1${NC}"
}

# 发送告警
send_alert() {
    local message="$1"

    # 发送到 Webhook（如 Slack、钉钉等）
    if [ -n "$WEBHOOK_URL" ]; then
        curl -X POST "$WEBHOOK_URL" \
            -H 'Content-Type: application/json' \
            -d "{\"text\":\"$message\"}" \
            2>/dev/null || true
    fi

    # 发送邮件（需要配置 sendmail）
    if [ -n "$ALERT_EMAIL" ] && command -v sendmail &> /dev/null; then
        echo "$message" | sendmail "$ALERT_EMAIL" || true
    fi
}

# 检查容器状态
check_container_status() {
    local container_name="$1"
    local status=$(docker inspect -f '{{.State.Status}}' "$container_name" 2>/dev/null || echo "not_found")

    if [ "$status" = "running" ]; then
        log_info "$container_name: 运行中"
        return 0
    elif [ "$status" = "not_found" ]; then
        log_error "$container_name: 容器不存在"
        send_alert "🚨 容器 $container_name 不存在"
        return 1
    else
        log_error "$container_name: 状态异常 ($status)"
        send_alert "🚨 容器 $container_name 状态异常: $status"
        return 1
    fi
}

# 检查容器健康
check_container_health() {
    local container_name="$1"
    local health=$(docker inspect -f '{{.State.Health.Status}}' "$container_name" 2>/dev/null || echo "none")

    if [ "$health" = "healthy" ]; then
        log_info "$container_name: 健康检查通过"
        return 0
    elif [ "$health" = "none" ]; then
        log_warning "$container_name: 未配置健康检查"
        return 0
    else
        log_error "$container_name: 健康检查失败 ($health)"
        send_alert "🚨 容器 $container_name 健康检查失败: $health"
        return 1
    fi
}

# 检查资源使用
check_resource_usage() {
    local container_name="$1"
    local cpu_threshold="${2:-80}"
    local mem_threshold="${3:-80}"

    local stats=$(docker stats --no-stream --format "{{.CPUPerc}},{{.MemPerc}}" "$container_name" 2>/dev/null || echo "0.00%,0.00%")
    local cpu=$(echo "$stats" | cut -d',' -f1 | sed 's/%//')
    local mem=$(echo "$stats" | cut -d',' -f2 | sed 's/%//')

    # 检查 CPU
    if (( $(echo "$cpu > $cpu_threshold" | bc -l) )); then
        log_warning "$container_name: CPU 使用率过高 (${cpu}%)"
        send_alert "⚠️ 容器 $container_name CPU 使用率过高: ${cpu}%"
    fi

    # 检查内存
    if (( $(echo "$mem > $mem_threshold" | bc -l) )); then
        log_warning "$container_name: 内存使用率过高 (${mem}%)"
        send_alert "⚠️ 容器 $container_name 内存使用率过高: ${mem}%"
    fi

    log_info "$container_name: CPU=${cpu}%, MEM=${mem}%"
}

# 检查磁盘空间
check_disk_space() {
    local threshold="${1:-80}"
    local usage=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')

    if [ "$usage" -gt "$threshold" ]; then
        log_warning "磁盘使用率过高 (${usage}%)"
        send_alert "⚠️ 磁盘使用率过高: ${usage}%"
    else
        log_info "磁盘使用率: ${usage}%"
    fi
}

# 检查日志错误
check_logs_for_errors() {
    local container_name="$1"
    local minutes="${2:-5}"

    local error_count=$(docker logs --since "${minutes}m" "$container_name" 2>&1 | grep -iE "error|fatal|panic" | wc -l)

    if [ "$error_count" -gt 10 ]; then
        log_warning "$container_name: 最近 ${minutes} 分钟内有 ${error_count} 条错误日志"
        send_alert "⚠️ 容器 $container_name 最近 ${minutes} 分钟内有 ${error_count} 条错误日志"
    else
        log_info "$container_name: 最近 ${minutes} 分钟内有 ${error_count} 条错误日志"
    fi
}

# 主监控函数
monitor() {
    echo "========================================"
    echo "Docker 容器健康监控"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================"
    echo ""

    local all_healthy=true

    # 检查主服务
    if ! check_container_status "codex-proxy"; then
        all_healthy=false
    else
        check_container_health "codex-proxy"
        check_resource_usage "codex-proxy" 80 80
        check_logs_for_errors "codex-proxy" 5
    fi

    echo ""

    # 检查数据库
    if ! check_container_status "codex-proxy-pg"; then
        all_healthy=false
    else
        check_container_health "codex-proxy-pg"
        check_resource_usage "codex-proxy-pg" 80 80
        check_logs_for_errors "codex-proxy-pg" 5
    fi

    echo ""

    # 检查磁盘空间
    check_disk_space 80

    echo ""
    echo "========================================"
    if [ "$all_healthy" = true ]; then
        log_info "所有服务运行正常"
    else
        log_error "部分服务异常，请检查"
    fi
    echo "========================================"
}

# 持续监控模式
watch_mode() {
    local interval="${1:-60}"

    echo "启动持续监控模式（间隔: ${interval}秒）"
    echo "按 Ctrl+C 退出"
    echo ""

    while true; do
        monitor
        echo ""
        echo "等待 ${interval} 秒..."
        sleep "$interval"
        clear
    done
}

# 显示帮助
show_help() {
    cat << EOF
Docker 容器健康监控脚本

用法: $0 [command] [options]

命令:
  check              执行一次健康检查
  watch [interval]   持续监控（默认间隔 60 秒）
  help               显示帮助

环境变量:
  ALERT_EMAIL        告警邮件地址
  WEBHOOK_URL        告警 Webhook URL（支持 Slack、钉钉等）

示例:
  $0 check                    # 执行一次检查
  $0 watch 30                 # 每 30 秒检查一次
  WEBHOOK_URL=https://... $0 check  # 配置告警

EOF
}

# 主函数
main() {
    local command="${1:-check}"

    case "$command" in
        check)
            monitor
            ;;
        watch)
            watch_mode "${2:-60}"
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            echo "未知命令: $command"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
