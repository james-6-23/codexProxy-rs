# Codex Proxy - 快速开始

## 🚀 一键部署

```bash
# 使用部署脚本（推荐）
./deploy.sh start

# 或手动执行
docker compose -f docker-compose.local.yml --env-file .env.local up -d
```

## 📋 部署步骤

### 1. 配置环境变量

首次运行 `./deploy.sh start` 会自动生成 `.env.local`，按需编辑：

```bash
CODEX_PORT=8080
ADMIN_SECRET=your-secure-password
TZ=Asia/Shanghai
```

### 2. 启动服务

```bash
# 方式一：使用脚本（推荐）
./deploy.sh build       # 构建镜像
./deploy.sh start       # 启动服务

# 方式二：手动命令
docker compose -f docker-compose.local.yml --env-file .env.local build
docker compose -f docker-compose.local.yml --env-file .env.local up -d
```

### 3. 访问服务

- **管理后台**: http://localhost:8080/admin
- **API 端点**: http://localhost:8080/v1/chat/completions
- **健康检查**: http://localhost:8080/health

## 📊 常用命令

```bash
./deploy.sh status          # 查看状态
./deploy.sh logs            # 查看日志
./deploy.sh logs codex-proxy # 查看指定服务日志
./deploy.sh restart         # 重启服务
./deploy.sh stop            # 停止服务
./deploy.sh backup          # 备份数据库
./deploy.sh restore <file>  # 恢复数据库
./deploy.sh health          # 健康检查
./deploy.sh help            # 查看全部命令
```

## 🔧 故障排查

### 端口被占用

```bash
# 修改 .env.local 中的端口
CODEX_PORT=8081
```

### 查看详细日志

```bash
./deploy.sh logs codex-proxy
```

### 重新构建

```bash
docker compose -f docker-compose.local.yml build --no-cache
```

## 📖 完整文档

详细部署文档请查看: [DOCKER_DEPLOY.md](./DOCKER_DEPLOY.md)

## 🆘 获取帮助

- 查看状态: `./deploy.sh status`
- 查看日志: `./deploy.sh logs`
- GitHub Issues: https://github.com/james-6-23/codexProxy-rs/issues
