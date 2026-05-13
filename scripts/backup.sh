#!/bin/bash
# PostgreSQL 数据库备份脚本

set -euo pipefail

# 配置
BACKUP_DIR="${BACKUP_DIR:-./backups}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-codex-proxy-pg}"
POSTGRES_USER="${POSTGRES_USER:-codex}"
POSTGRES_DB="${POSTGRES_DB:-codex2api}"
KEEP_DAYS="${KEEP_DAYS:-7}"

# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
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

# 创建备份
backup() {
    # 创建备份目录
    mkdir -p "$BACKUP_DIR"

    # 生成备份文件名
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_file="$BACKUP_DIR/backup_${timestamp}.sql.gz"

    log_info "开始备份数据库..."
    log_info "容器: $POSTGRES_CONTAINER"
    log_info "数据库: $POSTGRES_DB"
    log_info "备份文件: $backup_file"

    # 执行备份
    if docker exec "$POSTGRES_CONTAINER" pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" | gzip > "$backup_file"; then
        local size=$(du -h "$backup_file" | cut -f1)
        log_info "备份成功！文件大小: $size"

        # 清理旧备份
        cleanup_old_backups

        return 0
    else
        log_error "备份失败"
        rm -f "$backup_file"
        return 1
    fi
}

# 清理旧备份
cleanup_old_backups() {
    log_info "清理 ${KEEP_DAYS} 天前的备份..."

    local deleted=0
    while IFS= read -r file; do
        rm -f "$file"
        deleted=$((deleted + 1))
        log_info "已删除: $(basename "$file")"
    done < <(find "$BACKUP_DIR" -name "backup_*.sql.gz" -mtime +${KEEP_DAYS})

    if [ $deleted -eq 0 ]; then
        log_info "没有需要清理的旧备份"
    else
        log_info "已清理 $deleted 个旧备份"
    fi
}

# 列出备份
list_backups() {
    log_info "备份列表:"
    echo ""

    if [ ! -d "$BACKUP_DIR" ] || [ -z "$(ls -A "$BACKUP_DIR"/backup_*.sql.gz 2>/dev/null)" ]; then
        echo "  无备份文件"
        return
    fi

    local total_size=0
    local count=0

    while IFS= read -r file; do
        local size=$(du -h "$file" | cut -f1)
        local date=$(basename "$file" | sed 's/backup_\(.*\)\.sql\.gz/\1/' | sed 's/_/ /')
        echo "  [$((++count))] $date - $size - $(basename "$file")"
        total_size=$((total_size + $(du -k "$file" | cut -f1)))
    done < <(ls -t "$BACKUP_DIR"/backup_*.sql.gz 2>/dev/null)

    echo ""
    echo "  总计: $count 个备份，$(numfmt --to=iec-i --suffix=B $((total_size * 1024)))"
}

# 恢复备份
restore() {
    local backup_file="$1"

    if [ -z "$backup_file" ]; then
        log_error "请指定备份文件"
        echo ""
        list_backups
        exit 1
    fi

    if [ ! -f "$backup_file" ]; then
        log_error "备份文件不存在: $backup_file"
        exit 1
    fi

    log_warning "即将恢复数据库，当前数据将被覆盖！"
    log_warning "备份文件: $backup_file"
    read -p "确认继续? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        log_info "已取消"
        exit 0
    fi

    log_info "开始恢复数据库..."

    # 先备份当前数据
    log_info "先备份当前数据..."
    backup

    # 执行恢复
    if gunzip -c "$backup_file" | docker exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"; then
        log_info "恢复成功！"
        return 0
    else
        log_error "恢复失败"
        return 1
    fi
}

# 验证备份
verify() {
    local backup_file="$1"

    if [ -z "$backup_file" ]; then
        log_error "请指定备份文件"
        exit 1
    fi

    if [ ! -f "$backup_file" ]; then
        log_error "备份文件不存在: $backup_file"
        exit 1
    fi

    log_info "验证备份文件: $backup_file"

    # 检查文件是否为有效的 gzip 文件
    if ! gunzip -t "$backup_file" 2>/dev/null; then
        log_error "备份文件损坏或不是有效的 gzip 文件"
        return 1
    fi

    # 检查 SQL 内容
    if ! gunzip -c "$backup_file" | head -n 10 | grep -q "PostgreSQL"; then
        log_error "备份文件不包含有效的 PostgreSQL 数据"
        return 1
    fi

    log_info "备份文件验证通过"
    return 0
}

# 自动备份（用于 cron）
auto_backup() {
    log_info "执行自动备份..."

    if backup; then
        log_info "自动备份完成"
        exit 0
    else
        log_error "自动备份失败"
        exit 1
    fi
}

# 显示帮助
show_help() {
    cat << EOF
PostgreSQL 数据库备份脚本

用法: $0 <command> [options]

命令:
  backup             创建备份
  list               列出所有备份
  restore <file>     恢复备份
  verify <file>      验证备份文件
  cleanup            清理旧备份
  auto               自动备份（用于 cron）
  help               显示帮助

环境变量:
  BACKUP_DIR         备份目录（默认: ./backups）
  POSTGRES_CONTAINER PostgreSQL 容器名（默认: codex-proxy-pg）
  POSTGRES_USER      数据库用户（默认: codex）
  POSTGRES_DB        数据库名（默认: codex2api）
  KEEP_DAYS          保留天数（默认: 7）

示例:
  $0 backup                                    # 创建备份
  $0 list                                      # 列出备份
  $0 restore backups/backup_20260503_120000.sql.gz  # 恢复备份
  $0 verify backups/backup_20260503_120000.sql.gz   # 验证备份

Cron 示例（每天凌晨 2 点备份）:
  0 2 * * * cd /path/to/project && ./scripts/backup.sh auto >> logs/backup.log 2>&1

EOF
}

# 主函数
main() {
    local command="${1:-help}"

    case "$command" in
        backup)
            backup
            ;;
        list)
            list_backups
            ;;
        restore)
            restore "${2:-}"
            ;;
        verify)
            verify "${2:-}"
            ;;
        cleanup)
            cleanup_old_backups
            ;;
        auto)
            auto_backup
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

main "$@"
