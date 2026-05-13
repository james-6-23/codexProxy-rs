# Codex Proxy 本地 Docker 部署指南

## 📋 前置要求

- Docker 20.10+
- Docker Compose 2.0+
- 至少 4GB 可用内存
- 至少 10GB 可用磁盘空间

## 🚀 快速开始

### 1. 配置环境变量

编辑 `.env.local` 文件：

```bash
# 服务端口
CODEX_PORT=8080

# 管理后台密钥（建议设置）
ADMIN_SECRET=your-secure-secret-here

# 全局代理（可选）
# PROXY_URL=http://proxy.example.com:7890

# 时区
TZ=Asia/Shanghai
```

### 2. 构建并启动服务

```bash
# 构建镜像（首次或代码更新后）
docker compose -f docker-compose.local.yml --env-file .env.local build

# 启动服务
docker compose -f docker-compose.local.yml --env-file .env.local up -d

# 查看日志
docker compose -f docker-compose.local.yml logs -f codex-proxy
```

### 3. 访问服务

- **管理后台**: http://localhost:8080/admin
- **API 端点**: http://localhost:8080/v1/chat/completions
- **健康检查**: http://localhost:8080/health

## 📊 服务管理

### 查看状态
```bash
docker compose -f docker-compose.local.yml ps
```

### 查看日志
```bash
# 实时日志
docker compose -f docker-compose.local.yml logs -f

# 仅查看应用日志
docker compose -f docker-compose.local.yml logs -f codex-proxy

# 仅查看数据库日志
docker compose -f docker-compose.local.yml logs -f postgres
```

### 重启服务
```bash
# 重启所有服务
docker compose -f docker-compose.local.yml restart

# 仅重启应用
docker compose -f docker-compose.local.yml restart codex-proxy
```

### 停止服务
```bash
# 停止但保留数据
docker compose -f docker-compose.local.yml stop

# 停止并删除容器（保留数据卷）
docker compose -f docker-compose.local.yml down

# 停止并删除所有数据（危险操作！）
docker compose -f docker-compose.local.yml down -v
```

## 🔧 高级配置

### 设备指纹配置

在 `.env.local` 中添加：

```bash
# 固定设备指纹
STABILIZE_DEVICE_PROFILE=true
CODEX_USER_AGENT=codex_cli_rs/0.117.0 (Mac OS 15.5.0; arm64) Apple_Terminal/464
CODEX_OS=MacOS
CODEX_ARCH=arm64
```

### 数据库连接池配置

```bash
# 连接池大小（默认 256）
DB_POOL_SIZE=512
```

### 性能调优

```bash
# Rust 日志级别
RUST_LOG=info

# 时区设置
TZ=Asia/Shanghai
```

## 📦 数据持久化

数据存储在 Docker 卷中：

```bash
# 查看数据卷
docker volume ls | grep codex-proxy

# 备份数据库
docker exec codex-proxy-pg pg_dump -U codex codex2api > backup.sql

# 恢复数据库
docker exec -i codex-proxy-pg psql -U codex codex2api < backup.sql
```

## 🐛 故障排查

### 1. 容器无法启动

```bash
# 查看详细日志
docker compose -f docker-compose.local.yml logs codex-proxy

# 检查端口占用
lsof -i :8080
```

### 2. 数据库连接失败

```bash
# 检查数据库健康状态
docker compose -f docker-compose.local.yml ps postgres

# 手动连接数据库测试
docker exec -it codex-proxy-pg psql -U codex -d codex2api
```

### 3. 前端无法访问

```bash
# 检查前端是否正确嵌入
docker exec codex-proxy ls -la /usr/local/bin/

# 重新构建（清除缓存）
docker compose -f docker-compose.local.yml build --no-cache
```

### 4. 内存不足

```bash
# 查看容器资源使用
docker stats

# 限制容器内存（在 docker-compose.local.yml 中添加）
services:
  codex-proxy:
    deploy:
      resources:
        limits:
          memory: 2G
```

## 🔄 更新部署

```bash
# 1. 拉取最新代码
git pull

# 2. 重新构建镜像
docker compose -f docker-compose.local.yml build

# 3. 重启服务
docker compose -f docker-compose.local.yml up -d

# 4. 查看日志确认
docker compose -f docker-compose.local.yml logs -f codex-proxy
```

## 📈 性能监控

### 查看资源使用

```bash
# 实时监控
docker stats codex-proxy codex-proxy-pg

# 查看容器详情
docker inspect codex-proxy
```

### 数据库性能

```bash
# 连接到数据库
docker exec -it codex-proxy-pg psql -U codex -d codex2api

# 查看活跃连接
SELECT count(*) FROM pg_stat_activity;

# 查看表大小
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## 🔐 安全建议

1. **设置强密码**: 修改 `ADMIN_SECRET` 为复杂密码
2. **限制访问**: 使用防火墙限制 8080 端口访问
3. **定期备份**: 设置自动备份脚本
4. **更新镜像**: 定期更新基础镜像和依赖

## 📝 常用命令速查

```bash
# 启动
docker compose -f docker-compose.local.yml up -d

# 停止
docker compose -f docker-compose.local.yml down

# 重启
docker compose -f docker-compose.local.yml restart

# 查看日志
docker compose -f docker-compose.local.yml logs -f

# 进入容器
docker exec -it codex-proxy sh

# 查看状态
docker compose -f docker-compose.local.yml ps

# 清理未使用资源
docker system prune -a
```

## 🆘 获取帮助

- GitHub Issues: https://github.com/james-6-23/codexProxy-rs/issues
- 查看日志: `docker compose -f docker-compose.local.yml logs -f`
- 健康检查: `curl http://localhost:8080/health`
