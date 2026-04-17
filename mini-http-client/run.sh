#!/bin/bash

# 运行脚本

if [ ! -d "target/classes" ]; then
    echo "⚠️  Please compile first:"
    echo "  ./build.sh"
    exit 1
fi

echo "🚀 Running Mini HTTP Client Demo..."
echo ""

java -cp target/classes com.example.demo.Demo
