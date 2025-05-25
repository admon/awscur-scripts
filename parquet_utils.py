#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parquet转换工具模块
提供CSV/gzip到Parquet格式的转换功能
"""

import io
import json
import logging
import tempfile
import os
from typing import Dict, List, Optional, Set, Union

import pandas as pd

# 配置日志
logger = logging.getLogger(__name__)

class ParquetConverter:
    """处理文件格式转换
    使用简单直接的方法处理CSV到Parquet的转换
    特别关注时间字段和Map字段的正确处理
    """
    # 定义需要强制为字符串类型的字段
    STRING_COLUMNS = {
        'line_item_usage_account_id',
        'bill_payer_account_id',
        'line_item_resource_id',
        'product_region',
        'line_item_operation',
        'line_item_line_item_type',
        'product_product_family',
        'line_item_usage_type',
        'pricing_term',
        'product_from_account_id',
        'product_to_account_id',
        'pricing_plan_arn',
        'resource_id',
        'bill_invoice_id'  # 确保账单ID是字符串类型
    }
    
    # 时间字段列表
    TIME_FIELDS = [
        'line_item_usage_start_date',
        'line_item_usage_end_date',
        'bill_billing_period_start_date',
        'bill_billing_period_end_date'
    ]
    
    # Map字段列表
    MAP_FIELDS = [
        'cost_category',
        'discount',
        'product',
        'resource_tags'
    ]
    
    def __init__(self, memory_threshold_mb: int = 200):
        """初始化转换器
        
        Args:
            memory_threshold_mb: 内存处理阈值（MB），小于此值的文件直接在内存中处理
        """
        self.memory_threshold = memory_threshold_mb * 1024 * 1024  # 转换为字节
    
    def convert_csv_to_parquet(self, csv_content: bytes) -> Optional[bytes]:
        """将CSV内容转换为Parquet格式
        
        Args:
            csv_content: CSV文件内容（通常是gzip压缩的）
            
        Returns:
            Parquet格式的字节内容，失败时返回None
        """
        try:
            # 使用临时文件处理
            with tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False) as csv_tmp:
                csv_path = csv_tmp.name
                csv_tmp.write(csv_content)
                csv_tmp.flush()
            
            # 创建临时输出文件
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as parquet_tmp:
                parquet_path = parquet_tmp.name
            
            try:
                # 读取CSV为DataFrame
                df = pd.read_csv(csv_path, compression='gzip', low_memory=False)
                
                # 处理数据类型
                df = self._process_data_types(df)
                
                # 直接使用pandas保存为Parquet格式
                df.to_parquet(parquet_path, index=False)
                
                # 读取生成的Parquet文件
                with open(parquet_path, 'rb') as f:
                    parquet_content = f.read()
                
                return parquet_content
            
            finally:
                # 清理临时文件
                for path in [csv_path, parquet_path]:
                    if os.path.exists(path):
                        try:
                            os.unlink(path)
                        except Exception as e:
                            logger.warning(f"清理临时文件失败: {path}, 错误: {str(e)}")
        
        except Exception as e:
            logger.error(f"CSV转Parquet失败: {str(e)}")
            return None
    
    def _process_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理DataFrame的数据类型
        
        Args:
            df: 输入的DataFrame
            
        Returns:
            处理后的DataFrame
        """
        # 强制将特定字段转换为字符串类型
        for col in self.STRING_COLUMNS:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
                logger.debug(f"将字段 {col} 转换为字符串类型")
        
        # 处理时间字段 - 关键是转换为正确的格式
        for col in self.TIME_FIELDS:
            if col in df.columns:
                try:
                    # 转换为datetime类型
                    df[col] = pd.to_datetime(df[col])
                    # 移除时区信息
                    if df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)
                    # 将datetime转换为指定格式的字符串
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
                    logger.debug(f"将字段 {col} 转换为格式化字符串: {df[col].iloc[0] if not df[col].empty else ''}")
                except Exception as e:
                    logger.error(f"转换时间字段失败 {col}: {str(e)}")
        
        # 处理Map字段 - 尝试保持正确的JSON格式
        for field in self.MAP_FIELDS:
            if field in df.columns:
                try:
                    # 先将空值替换为空字典字符串
                    df[field] = df[field].fillna('{}')
                    
                    # 尝试确保每个值都是有效的JSON字符串
                    def ensure_json_string(val):
                        if pd.isna(val) or val == '':
                            return '{}'
                        
                        # 如果已经是字典对象，转换为JSON字符串
                        if isinstance(val, dict):
                            return json.dumps(val)
                            
                        # 如果是字符串，尝试解析并重新格式化
                        if isinstance(val, str):
                            try:
                                # 如果已经是JSON字符串，解析并重新格式化
                                if val.strip() and val.strip()[0] == '{':
                                    return json.dumps(json.loads(val))
                                else:
                                    # 不是JSON对象，创建一个包含原始值的字典
                                    return json.dumps({'value': val})
                            except:
                                # 解析失败，创建一个包含原始值的字典
                                return json.dumps({'value': val})
                        
                        # 其他类型，创建一个包含原始值的字典
                        return json.dumps({'value': str(val)})
                    
                    # 应用到每一行
                    df[field] = df[field].apply(ensure_json_string)
                    
                    logger.debug(f"将字段 {field} 转换为JSON字符串: {df[field].iloc[0] if not df[field].empty else '{}'}")
                    
                except Exception as e:
                    logger.error(f"转换Map字段失败 {field}: {str(e)}")
                    # 如果转换失败，将其设置为空JSON对象
                    df[field] = df[field].apply(lambda x: '{}' if pd.isna(x) or x == '' else str(x))
        
        return df

def parse_json_or_default(value):
    """解析JSON字符串，如果失败则返回默认值
    
    Args:
        value: 要解析的JSON字符串
                
    Returns:
        解析后的字典，或者默认值
    """
    if pd.isna(value) or value == '':
        return {'_empty': ''}
        
    try:
        # 如果已经是字典类型，直接返回
        if isinstance(value, dict):
            return value if value else {'_empty': ''}
                
        # 尝试解析JSON字符串
        result = json.loads(value)
        return result if result else {'_empty': ''}
    except Exception:
        # 解析失败，返回包含原始值的字典
        return {'value': str(value)}
