# syntax=docker/dockerfile:1

# ============================================================
# Stage 1: 构建前端 (React + Vite)
# ============================================================
FROM --platform=$BUILDPLATFORM node:20-alpine AS frontend-builder

WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund
COPY frontend/ .
RUN npm run build

# ============================================================
# Stage 2: 构建 Rust 后端（前端产物嵌入二进制）
# ============================================================
FROM --platform=$BUILDPLATFORM rust:1-alpine AS rust-builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

WORKDIR /app

# 依赖缓存层：先编译依赖，不编译业务代码
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs && \
    mkdir -p frontend/dist && touch frontend/dist/.keep && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src frontend

# 拷贝前端构建产物
COPY --from=frontend-builder /frontend/dist ./frontend/dist

# 拷贝业务源码，正式编译
COPY src/ src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp target/release/codex-proxy /codex-proxy

# ============================================================
# Stage 3: 最终运行镜像
# ============================================================
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata

COPY --from=rust-builder /codex-proxy /usr/local/bin/codex-proxy

EXPOSE 8080
ENTRYPOINT ["codex-proxy"]
