# syntax=docker/dockerfile:1.4

# ============================================================
# Stage 1: 构建前端 (React + Vite)
# ============================================================
FROM --platform=$BUILDPLATFORM node:20-alpine AS frontend-builder

WORKDIR /frontend

# 复制依赖文件
COPY frontend/package.json frontend/package-lock.json ./

# 安装依赖（使用缓存）
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund --prefer-offline

# 复制源码并构建
COPY frontend/ .
RUN npm run build && \
    # 验证构建产物
    test -d dist && test -f dist/index.html

# ============================================================
# Stage 2: 构建 Rust 后端
# ============================================================
FROM --platform=$BUILDPLATFORM rust:1-alpine AS rust-builder

# 安装构建依赖
RUN apk add --no-cache \
    musl-dev \
    pkgconfig \
    openssl-dev \
    openssl-libs-static

WORKDIR /app

# 复制依赖清单
COPY Cargo.toml Cargo.lock ./

# 创建虚拟源码以缓存依赖编译
RUN mkdir -p src && \
    echo "fn main(){}" > src/main.rs && \
    mkdir -p frontend/dist && \
    touch frontend/dist/.keep && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src frontend

# 复制前端构建产物
COPY --from=frontend-builder /frontend/dist ./frontend/dist

# 复制源码并编译
COPY src/ src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp target/release/codex-proxy /codex-proxy && \
    strip /codex-proxy

# ============================================================
# Stage 3: 最终运行镜像
# ============================================================
FROM alpine:3.21

# 安装运行时依赖
RUN apk --no-cache add \
    ca-certificates \
    tzdata \
    tini && \
    # 创建非 root 用户
    addgroup -g 1000 codex && \
    adduser -D -u 1000 -G codex codex

# 复制二进制文件
COPY --from=rust-builder /codex-proxy /usr/local/bin/codex-proxy

# 设置权限
RUN chmod +x /usr/local/bin/codex-proxy

# 切换到非 root 用户
USER codex

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8080/health || exit 1

EXPOSE 8080

# 使用 tini 作为 init 进程
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["codex-proxy"]
