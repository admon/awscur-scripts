#!/usr/bin/env python3
"""
CUR数据文件字段字节占用统计脚本
支持Parquet格式的AWS CUR数据文件分析
"""

import pandas as pd
import pyarrow.parquet as pq
import os
import sys
from pathlib import Path
import argparse

def get_column_memory_usage(df):
    """计算DataFrame中每列的内存使用量"""
    memory_usage = {}
    
    for col in df.columns:
        # 获取列的内存使用量（字节）
        col_memory = df[col].memory_usage(deep=True)
        memory_usage[col] = col_memory
    
    return memory_usage

def analyze_parquet_metadata(file_path):
    """分析Parquet文件的元数据以获取列信息"""
    try:
        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata
        schema = parquet_file.schema
        
        column_info = {}
        
        # 获取每个列组的统计信息
        for row_group_idx in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_idx)
            
            for col_idx in range(row_group.num_columns):
                col_meta = row_group.column(col_idx)
                col_name = schema.names[col_idx]
                
                if col_name not in column_info:
                    column_info[col_name] = {
                        'total_compressed_size': 0,
                        'total_uncompressed_size': 0,
                        'data_type': str(schema.field(col_idx).type)
                    }
                
                column_info[col_name]['total_compressed_size'] += col_meta.total_compressed_size
                column_info[col_name]['total_uncompressed_size'] += col_meta.total_uncompressed_size
        
        return column_info, metadata.num_rows
    
    except Exception as e:
        print(f"无法读取Parquet元数据: {e}")
        return None, 0

def format_bytes(bytes_value):
    """格式化字节数为可读格式"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} TB"

def analyze_cur_file(file_path, sample_rows=None):
    """分析CUR文件的字段字节占用"""
    
    print(f"正在分析文件: {file_path}")
    print("=" * 60)
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"错误: 文件 {file_path} 不存在")
        return
    
    file_size = os.path.getsize(file_path)
    print(f"文件大小: {format_bytes(file_size)}")
    print()
    
    try:
        # 首先分析Parquet元数据
        column_metadata, total_rows = analyze_parquet_metadata(file_path)
        
        if column_metadata:
            print("=== Parquet文件列压缩信息 ===")
            print(f"总行数: {total_rows:,}")
            print()
            
            # 按压缩大小排序
            sorted_columns = sorted(column_metadata.items(), 
                                  key=lambda x: x[1]['total_compressed_size'], 
                                  reverse=True)
            
            total_compressed = sum(info['total_compressed_size'] for _, info in sorted_columns)
            total_uncompressed = sum(info['total_uncompressed_size'] for _, info in sorted_columns)
            
            print(f"{'字段名':<30} {'数据类型':<20} {'压缩大小':<15} {'原始大小':<15} {'压缩比':<10}")
            print("-" * 100)
            
            for col_name, info in sorted_columns:
                compressed_size = info['total_compressed_size']
                uncompressed_size = info['total_uncompressed_size']
                compression_ratio = uncompressed_size / compressed_size if compressed_size > 0 else 0
                
                print(f"{col_name:<30} {info['data_type']:<20} "
                      f"{format_bytes(compressed_size):<15} "
                      f"{format_bytes(uncompressed_size):<15} "
                      f"{compression_ratio:.2f}x")
            
            print("-" * 100)
            print(f"{'总计':<30} {'':<20} "
                  f"{format_bytes(total_compressed):<15} "
                  f"{format_bytes(total_uncompressed):<15} "
                  f"{total_uncompressed/total_compressed:.2f}x")
            print()
        
        # 读取样本数据进行内存使用量分析
        print("=== 内存使用量分析 ===")
        if sample_rows:
            print(f"读取前 {sample_rows} 行进行分析...")
            df = pd.read_parquet(file_path, nrows=sample_rows)
        else:
            print("读取完整文件进行分析...")
            df = pd.read_parquet(file_path)
        
        print(f"数据形状: {df.shape[0]:,} 行 × {df.shape[1]} 列")
        print()
        
        # 获取内存使用量
        memory_usage = get_column_memory_usage(df)
        
        # 按内存使用量排序
        sorted_memory = sorted(memory_usage.items(), key=lambda x: x[1], reverse=True)
        
        total_memory = sum(memory_usage.values())
        
        print(f"{'字段名':<30} {'内存使用':<15} {'占比':<10} {'数据类型':<20}")
        print("-" * 80)
        
        for col_name, memory_bytes in sorted_memory:
            percentage = (memory_bytes / total_memory) * 100
            dtype = str(df[col_name].dtype)
            print(f"{col_name:<30} {format_bytes(memory_bytes):<15} "
                  f"{percentage:.1f}%{'':<5} {dtype:<20}")
        
        print("-" * 80)
        print(f"{'总计':<30} {format_bytes(total_memory):<15} {'100.0%':<10}")
        print()
        
        # 显示一些基本统计信息
        print("=== 数据类型统计 ===")
        dtype_counts = df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"{str(dtype):<20}: {count} 列")
        
        # 显示空值统计
        print("\n=== 空值统计 (前10个字段) ===")
        null_counts = df.isnull().sum().sort_values(ascending=False).head(10)
        for col, null_count in null_counts.items():
            null_percentage = (null_count / len(df)) * 100
            print(f"{col:<30}: {null_count:,} ({null_percentage:.1f}%)")
            
    except Exception as e:
        print(f"读取文件时出错: {e}")
        print("请确保文件是有效的Parquet格式")

def main():
    parser = argparse.ArgumentParser(description='分析CUR数据文件中各字段的字节占用')
    parser.add_argument('file_path', help='CUR数据文件路径')
    parser.add_argument('--sample', type=int, 
                       help='仅分析指定行数的样本数据（用于大文件）')
    
    args = parser.parse_args()
    
    analyze_cur_file(args.file_path, args.sample)

if __name__ == "__main__":
    # 如果直接运行且没有命令行参数，提供交互式输入
    if len(sys.argv) == 1:
        file_path = input("请输入CUR文件路径: ").strip()
        sample_input = input("是否只分析样本数据？(输入行数，或回车分析全部): ").strip()
        
        sample_rows = None
        if sample_input:
            try:
                sample_rows = int(sample_input)
            except ValueError:
                print("无效的行数，将分析全部数据")
        
        analyze_cur_file(file_path, sample_rows)
    else:
        main()
