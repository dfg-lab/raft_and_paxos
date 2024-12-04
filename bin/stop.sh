#!/usr/bin/env bash

#!/bin/bash

# 3つのプロセスIDを引数として受け取る
pid1="$1"
pid2="$2"
pid3="$3"

# 各プロセスをkillする
kill "$pid1"
kill "$pid2"
kill "$pid3"

# 終了ステータスを確認
if [ $? -eq 0 ]; then
  echo "プロセスを正常に終了しました。"
else
  echo "プロセスの終了に失敗しました。"
fi