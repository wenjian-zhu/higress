#!/usr/bin/env bash
set -e

# ===============================
# Higress WASM 插件构建脚本
# 使用 Docker + Bazel 自动构建并导出 .wasm 文件
# ===============================

# 1️⃣ 参数检查
if [ -z "$1" ]; then
  echo "❌ 用法: $0 <PLUGIN_NAME>"
  echo "示例: ./build.sh my_plugin"
  exit 1
fi

PLUGIN_NAME=$1
OUT_DIR="./out"

# 2️⃣ 构建输出目录
mkdir -p "$OUT_DIR"

# 3️⃣ 运行 Docker 构建并直接导出结果
echo "🚀 开始构建 Higress 插件: $PLUGIN_NAME"
docker build \
  --build-arg PLUGIN_NAME="$PLUGIN_NAME" \
  --output "type=local,dest=${OUT_DIR}" \
  -t "${PLUGIN_NAME}-builder" .

# 4️⃣ 检查输出结果
if [ -f "${OUT_DIR}/plugin.wasm" ]; then
  mv "${OUT_DIR}/plugin.wasm" "${OUT_DIR}/${PLUGIN_NAME}.wasm"
  echo "✅ 构建成功: ${OUT_DIR}/${PLUGIN_NAME}.wasm"
else
  echo "❌ 构建失败：未找到输出文件 plugin.wasm"
  exit 1
fi
