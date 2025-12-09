#!/bin/bash
# 性能诊断脚本 - 快速识别瓶颈

echo "========================================="
echo "  OceanBase Comparator 性能诊断"
echo "========================================="
echo

# 1. 检查dbcat_output位置
echo "[1] 检查缓存目录..."
if [ -d "dbcat_output" ]; then
    CACHE_DIR=$(readlink -f dbcat_output)
    echo "  缓存目录: $CACHE_DIR"
    
    # 检查是否在网络存储上
    FS_TYPE=$(df -T "$CACHE_DIR" | tail -1 | awk '{print $2}')
    echo "  文件系统: $FS_TYPE"
    
    if [[ "$FS_TYPE" == "nfs"* ]] || [[ "$FS_TYPE" == "cifs" ]]; then
        echo "  ⚠️  警告: 使用网络存储，建议迁移到本地SSD"
    elif [[ "$FS_TYPE" == "ext4" ]] || [[ "$FS_TYPE" == "xfs" ]]; then
        echo "  ✓ 本地文件系统"
    fi
    
    # 统计文件数量
    FILE_COUNT=$(find dbcat_output/flat_cache -type f 2>/dev/null | wc -l)
    echo "  缓存文件数: $FILE_COUNT"
else
    echo "  缓存目录不存在"
fi
echo

# 2. 测试文件读取性能
echo "[2] 测试文件IO性能..."
if [ -d "dbcat_output/flat_cache" ]; then
    TEST_FILE=$(find dbcat_output/flat_cache -type f | head -1)
    if [ -n "$TEST_FILE" ]; then
        echo "  测试文件: $TEST_FILE"
        
        # 测试10次读取
        TOTAL_TIME=0
        for i in {1..10}; do
            START=$(date +%s.%N)
            cat "$TEST_FILE" > /dev/null
            END=$(date +%s.%N)
            ELAPSED=$(echo "$END - $START" | bc)
            TOTAL_TIME=$(echo "$TOTAL_TIME + $ELAPSED" | bc)
        done
        AVG_TIME=$(echo "scale=3; $TOTAL_TIME / 10" | bc)
        
        echo "  平均读取耗时: ${AVG_TIME}s"
        
        if (( $(echo "$AVG_TIME > 0.1" | bc -l) )); then
            echo "  ⚠️  警告: 文件读取较慢 (>0.1s)"
            echo "  建议: 设置 cache_parallel_workers=4-8"
        else
            echo "  ✓ 文件读取正常"
        fi
    fi
fi
echo

# 3. 检查配置
echo "[3] 检查配置..."
if [ -f "config.ini" ]; then
    CACHE_WORKERS=$(grep "cache_parallel_workers" config.ini | cut -d'=' -f2 | tr -d ' ')
    CHUNK_SIZE=$(grep "dbcat_chunk_size" config.ini | cut -d'=' -f2 | tr -d ' ')
    
    echo "  cache_parallel_workers: ${CACHE_WORKERS:-未设置(默认1)}"
    echo "  dbcat_chunk_size: ${CHUNK_SIZE:-未设置(默认150)}"
    
    if [ -z "$CACHE_WORKERS" ] || [ "$CACHE_WORKERS" == "1" ]; then
        echo "  💡 建议: 如果磁盘IO慢，设置 cache_parallel_workers=4"
    fi
fi
echo

# 4. 系统资源
echo "[4] 系统资源..."
echo "  CPU核心数: $(nproc)"
echo "  可用内存: $(free -h | grep Mem | awk '{print $7}')"
echo "  磁盘IO:"
iostat -x 1 2 | tail -n +4 | head -5 | awk '{if(NR>1) printf "    %s: await=%.1fms util=%.1f%%\n", $1, $10, $14}'
echo

# 5. 建议
echo "========================================="
echo "  性能优化建议"
echo "========================================="

if [[ "$FS_TYPE" == "nfs"* ]] || [[ "$FS_TYPE" == "cifs" ]]; then
    echo "1. 【高优先级】迁移缓存到本地SSD"
    echo "   mkdir /local/ssd/dbcat_cache"
    echo "   mv dbcat_output/* /local/ssd/dbcat_cache/"
    echo "   rm -rf dbcat_output"
    echo "   ln -s /local/ssd/dbcat_cache dbcat_output"
    echo
fi

if [ -z "$CACHE_WORKERS" ] || [ "$CACHE_WORKERS" == "1" ]; then
    echo "2. 启用并行缓存加载"
    echo "   在 config.ini 的 [SETTINGS] 中添加:"
    echo "   cache_parallel_workers = 4"
    echo
fi

echo "3. 如果仍然很慢，考虑清理缓存重新导出"
echo "   rm -rf dbcat_output/*"
echo "   python3 schema_diff_reconciler.py"
echo

echo "========================================="
