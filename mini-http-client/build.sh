#!/bin/bash

# 编译脚本

echo "🔨 Compiling Mini HTTP Client..."

# 创建输出目录
mkdir -p target/classes

# 编译所有 Java 文件
javac -d target/classes \
  src/main/java/com/example/future/*.java \
  src/main/java/com/example/network/*.java \
  src/main/java/com/example/api/*.java \
  src/main/java/com/example/client/*.java \
  src/main/java/com/example/demo/*.java

if [ $? -eq 0 ]; then
    echo "✅ Compilation successful!"
    echo ""
    echo "Run the demo with:"
    echo "  ./run.sh"
else
    echo "❌ Compilation failed!"
    exit 1
fi
