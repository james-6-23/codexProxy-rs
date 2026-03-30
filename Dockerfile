# syntax=docker/dockerfile:1

# ============================================================
# Stage 1: 构建前端 (React + Vite)
# ============================================================
FROM --platform=$BUILDPLATFORM node:20-alpine AS frontend-builder

WORKDIR /frontend
COPY codex2api/frontend/package.json codex2api/frontend/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund
COPY codex2api/frontend/ .
RUN npm run build

# ============================================================
# Stage 2: 构建 Rust 后端
# ============================================================
FROM --platform=$BUILDPLATFORM rust:1.83-alpine AS rust-builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

WORKDIR /app
# 先拷贝依赖清单，利用 Docker 层缓存
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo "fn main(){}" > src/main.rs && \
    mkdir -p frontend/dist && touch frontend/dist/.keep && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src frontend

# 拷贝前端产物 + 源码，正式编译
COPY --from=frontend-builder /frontend/dist ./frontend/dist
COPY src/ src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp target/release/codex-proxy /codex-proxy

# ============================================================
# Stage 3: 最终运行镜像（~15MB）
# ============================================================
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata

COPY --from=rust-builder /codex-proxy /usr/local/bin/codex-proxy

EXPOSE 8080

ENTRYPOINT ["codex-proxy"]
