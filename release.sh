#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Talon-Bin Release Script
# 用法: ./release.sh v0.1.24
# ─────────────────────────────────────────────────────────────
set -euo pipefail

# ── 颜色 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
err()  { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }
step() { echo -e "\n${CYAN}══ $* ══${NC}"; }

# ── 参数校验 ──
TAG="${1:-}"
if [[ -z "$TAG" ]]; then
    echo "用法: $0 <version-tag>"
    echo "示例: $0 v0.1.24"
    exit 1
fi

# 校验 tag 格式
if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    err "Tag 格式错误: $TAG (期望 vX.Y.Z)"
fi

VERSION="${TAG#v}"  # 去掉 v 前缀: v0.1.24 → 0.1.24

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── 代理配置 ──
export https_proxy=http://127.0.0.1:7890
export http_proxy=http://127.0.0.1:7890
export all_proxy=socks5://127.0.0.1:7890

# ── Step 1: 检查工作区状态 ──
step "检查 Git 状态"

if [[ -n "$(git status --porcelain)" ]]; then
    warn "talon-bin 有未提交的更改:"
    git status --short
    read -p "继续发布？[y/N] " -n 1 -r
    echo
    [[ $REPLY =~ ^[Yy]$ ]] || exit 0
fi

# 确保在 main 分支
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$BRANCH" != "main" ]]; then
    err "当前在 $BRANCH 分支，请切换到 main: git checkout main"
fi

# 检查 tag 是否已存在
if git rev-parse "$TAG" >/dev/null 2>&1; then
    err "Tag $TAG 已存在！请使用新的版本号。"
fi

# ── Step 2: 更新版本号 ──
step "更新版本号 → $VERSION"

# talon-sys/Cargo.toml
CARGO_TOML="$SCRIPT_DIR/talon-sys/Cargo.toml"
if grep -q "version = \"$VERSION\"" "$CARGO_TOML"; then
    log "Cargo.toml 已是 $VERSION"
else
    sed -i '' "s/^version = \".*\"/version = \"$VERSION\"/" "$CARGO_TOML"
    log "Cargo.toml → $VERSION"
fi

# talon-sys/build.rs (TALON_LIB_VERSION)
BUILD_RS="$SCRIPT_DIR/talon-sys/build.rs"
if grep -q "TALON_LIB_VERSION: &str = \"$VERSION\"" "$BUILD_RS"; then
    log "build.rs TALON_LIB_VERSION 已是 $VERSION"
else
    sed -i '' "s/TALON_LIB_VERSION: &str = \"[^\"]*\"/TALON_LIB_VERSION: \&str = \"$VERSION\"/" "$BUILD_RS"
    log "build.rs TALON_LIB_VERSION → $VERSION"
fi

# ── Step 3: 提交 ──
step "提交更改"

git add -A
if git diff --cached --quiet; then
    log "无需提交（所有更改已在上一次 commit 中）"
else
    git commit -m "feat(sys): bump $TAG — pre-built binary release"
    log "已提交"
fi

# ── Step 4: 打 Tag ──
step "创建 Tag: $TAG"
git tag -a "$TAG" -m "Release $TAG"
log "Tag $TAG 已创建"

# ── Step 5: 推送 ──
step "推送到 GitHub"
git push origin main
log "main 已推送"
git push origin "$TAG"
log "Tag $TAG 已推送"

# ── Step 6: 等待 CI ──
step "触发 CI 构建"
echo ""
echo "  GitHub Actions 将自动构建 8 个平台的预编译库并创建 Release。"
echo ""
echo "  查看进度:"
echo "    https://github.com/darkmice/talon-bin/actions"
echo ""
echo "  构建完成后，更新 superclaw 依赖:"
echo "    Cargo.toml: talon = { git = \"...\", tag = \"$TAG\", ... }"
echo ""

log "发布 $TAG 完成！🎉"
