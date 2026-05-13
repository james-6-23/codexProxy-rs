#!/bin/bash
# Codex Proxy 增强部署脚本
# 支持本地构建、生产部署、备份恢复等功能

set -euo pipefail

# ============================================================
# 配置
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.local.yml}"
ENV_FILE="${ENV_FILE:-.env.local}"
PROJECT_NAME="codex-proxy"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================
# 工具函数
# ============================================================
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

check_dependencies() {
    local missing=0

    if ! command -v docker &> /dev/null; then
        log_error "未安装 Docker"
        missing=1
    fi

    if ! command -v docker compose &> /dev/null; then
        log_error "未安装 Docker Compose"
        missing=1
    fi

    if [ $missing -eq 1 ]; then
        exit 1
    fi
}

create_env_file() {
    if [ ! -f "$ENV_FILE" ]; then
        log_info "创建默认配置文件 $ENV_FILE"
        cat > "$ENV_FILE" << 'EOF'
# Codex Proxy 配置文件

# 应用配置
CODEX_PORT=8080
ADMIN_SECRET=change-me-in-production
PROXY_URL=

# 数据库配置
POSTGRES_USER=codex
POSTGRES_PASSWORD=codex
POSTGRES_DB=codex2api

# 系统配置
TZ=Asia/Shanghai
RUST_LOG=info
RUST_BACKTRACE=0

# 备份配置
BACKUP_KEEP_DAYS=7
EOF
        log_warning "请编辑 $ENV_FILE 设置 ADMIN_SECRET 和其他配置"
        return 1
    fi
    return 0
}

check_env_file() {
    if [ ! -f "$ENV_FILE" ]; then
        log_error "配置文件 $ENV_FILE 不存在"
        create_env_file
        exit 1
    fi

    # 检查关键配置
    if grep -q "ADMIN_SECRET=change-me-in-production" "$ENV_FILE"; then
        log_warning "请修改 ADMIN_SECRET 为安全的密钥"
    fi
}

# ============================================================
# 操作函数
# ============================================================

# 构建镜像
build() {
    log_info "开始构建镜像..."
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" build "$@"
    log_success "镜像构建完成"
}

# 启动服务
start() {
    log_info "启动服务..."
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d "$@"
    log_success "服务已启动"

    # 等待服务就绪
    log_info "等待服务就绪..."
    sleep 5

    # 显示状态
    status

    echo ""
    log_success "部署完成！"
    echo "📊 管理后台: http://localhost:${CODEX_PORT:-8080}/admin"
    echo "🔍 健康检查: http://localhost:${CODEX_PORT:-8080}/health"
}

# 停止服务
stop() {
    log_info "停止服务..."
    docker compose -f "$COMPOSE_FILE" down
    log_success "服务已停止"
}

# 重启服务
restart() {
    log_info "重启服务..."
    docker compose -f "$COMPOSE_FILE" restart "$@"
    log_success "服务已重启"
}

# 查看状态
status() {
    echo ""
    log_info "服务状态:"
    docker compose -f "$COMPOSE_FILE" ps

    echo ""
    log_info "健康检查:"
    docker ps --filter "name=${PROJECT_NAME}" --format "table {{.Names}}\t{{.Status}}"

    echo ""
    log_info "资源使用:"
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" \
        $(docker ps --filter "name=${PROJECT_NAME}" -q)
}

# 查看日志
logs() {
    local service="${1:-}"
    if [ -z "$service" ]; then
        docker compose -f "$COMPOSE_FILE" logs -f --tail=100
    else
        docker compose -f "$COMPOSE_FILE" logs -f --tail=100 "$service"
    fi
}

# 进入容器
shell() {
    local service="${1:-codex-proxy}"
    log_info "进入 $service 容器..."
    docker compose -f "$COMPOSE_FILE" exec "$service" sh
}

# 备份数据库
backup() {
    local backup_dir="./backups"
    mkdir -p "$backup_dir"

    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_file="$backup_dir/backup_${timestamp}.sql.gz"

    log_info "开始备份数据库..."
    docker compose -f "$COMPOSE_FILE" exec -T postgres \
        pg_dump -U "${POSTGRES_USER:-codex}" -d "${POSTGRES_DB:-codex2api}" | gzip > "$backup_file"

    log_success "备份完成: $backup_file"

    # 显示备份大小
    local size=$(du -h "$backup_file" | cut -f1)
    echo "备份大小: $size"
}

# 恢复数据库
restore() {
    local backup_file="$1"

    if [ -z "$backup_file" ]; then
        log_error "请指定备份文件"
        echo "用法: $0 restore <backup_file>"
        echo ""
        echo "可用备份:"
        ls -lh ./backups/*.sql.gz 2>/dev/null || echo "无备份文件"
        exit 1
    fi

    if [ ! -f "$backup_file" ]; then
        log_error "备份文件不存在: $backup_file"
        exit 1
    fi

    log_warning "即将恢复数据库，当前数据将被覆盖！"
    read -p "确认继续? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        log_info "已取消"
        exit 0
    fi

    log_info "开始恢复数据库..."
    gunzip -c "$backup_file" | docker compose -f "$COMPOSE_FILE" exec -T postgres \
        psql -U "${POSTGRES_USER:-codex}" -d "${POSTGRES_DB:-codex2api}"

    log_success "恢复完成"
}

# 清理资源
clean() {
    log_warning "即将删除所有容器、镜像和数据卷！"
    read -p "确认继续? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        log_info "已取消"
        exit 0
    fi

    log_info "清理资源..."
    docker compose -f "$COMPOSE_FILE" down -v --rmi all
    log_success "清理完成"
}

# 更新服务
update() {
    log_info "更新服务..."

    # 备份数据库
    backup

    # 拉取最新代码（如果是 git 仓库）
    if [ -d .git ]; then
        log_info "拉取最新代码..."
        git pull
    fi

    # 重新构建
    build

    # 重启服务
    docker compose -f "$COMPOSE_FILE" up -d

    log_success "更新完成"
}

# 健康检查
health() {
    log_info "执行健康检查..."

    local port="${CODEX_PORT:-8080}"
    local health_url="http://localhost:${port}/health"

    if command -v curl &> /dev/null; then
        curl -f "$health_url" && log_success "健康检查通过" || log_error "健康检查失败"
    elif command -v wget &> /dev/null; then
        wget -q -O- "$health_url" && log_success "健康检查通过" || log_error "健康检查失败"
    else
        log_warning "未安装 curl 或 wget，无法执行健康检查"
    fi
}

# 显示帮助
show_help() {
    cat << EOF
Codex Proxy 部署脚本

用法: $0 <command> [options]

命令:
  build              构建镜像
  start              启动服务
  stop               停止服务
  restart [service]  重启服务（可指定服务名）
  status             查看状态
  logs [service]     查看日志（可指定服务名）
  shell [service]    进入容器（默认: codex-proxy）
  backup             备份数据库
  restore <file>     恢复数据库
  update             更新服务（拉取代码、重新构建、重启）
  health             健康检查
  clean              清理所有资源（危险操作）
  help               显示帮助

示例:
  $0 build                    # 构建镜像
  $0 start                    # 启动服务
  $0 logs codex-proxy         # 查看主服务日志
  $0 backup                   # 备份数据库
  $0 restore backups/backup_20260503_120000.sql.gz  # 恢复数据库

环境变量:
  COMPOSE_FILE    Docker Compose 文件路径（默认: docker-compose.local.yml）
  ENV_FILE        环境变量文件路径（默认: .env.local）

EOF
}

# ============================================================
# 主函数
# ============================================================
main() {
    # 检查依赖
    check_dependencies

    # 创建必要的目录
    mkdir -p backups logs

    # 解析命令
    local command="${1:-help}"
    shift || true

    case "$command" in
        build)
            check_env_file
            build "$@"
            ;;
        start)
            create_env_file || true
            check_env_file
            start "$@"
            ;;
        stop)
            stop
            ;;
        restart)
            restart "$@"
            ;;
        status)
            status
            ;;
        logs)
            logs "$@"
            ;;
        shell)
            shell "$@"
            ;;
        backup)
            backup
            ;;
        restore)
            restore "$@"
            ;;
        update)
            update
            ;;
        health)
            health
            ;;
        clean)
            clean
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "未知命令: $command"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# 运行主函数
main "$@"
