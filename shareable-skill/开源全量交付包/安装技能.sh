#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")" && pwd)/skills"
TARGET_DIR="${HOME}/.codex/skills"

mkdir -p "$TARGET_DIR"
cp -R "$SRC_DIR/zhongdong-congling-gongcheng" "$TARGET_DIR/"
cp -R "$SRC_DIR/zhongdong-zongskill-anzhuang-diaoyong" "$TARGET_DIR/"

echo "技能安装完成："
echo "- $TARGET_DIR/zhongdong-congling-gongcheng"
echo "- $TARGET_DIR/zhongdong-zongskill-anzhuang-diaoyong"
echo "请重新打开AI会话后再触发技能。"
