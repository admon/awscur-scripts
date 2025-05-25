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
from typing import Dict, List, Optional, Set, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 配置日志
logger = logging.getLogger(__name__)

class ParquetConverter:
    """处理文件格式转换"""
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
        # 添加其他应该被视为字符串的字段
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
            # 使用BytesIO直接在内存中处理小文件
            if len(csv_content) < self.memory_threshold:  # 小于阈值的文件直接在内存中处理
                logger.debug(f"使用内存处理CSV文件，大小: {len(csv_content)/1024/1024:.2f}MB")
                return self._process_in_memory(csv_content)
            else:
                logger.debug(f"使用临时文件处理CSV文件，大小: {len(csv_content)/1024/1024:.2f}MB")
                return self._process_with_temp_file(csv_content)
        except Exception as e:
            logger.error(f"CSV转Parquet失败: {str(e)}")
            return None
    
    def _process_in_memory(self, csv_content: bytes) -> Optional[bytes]:
        """在内存中处理CSV到Parquet的转换
        
        Args:
            csv_content: CSV文件内容
            
        Returns:
            Parquet格式的字节内容
        """
        try:
            # 使用BytesIO在内存中处理
            with io.BytesIO(csv_content) as csv_buffer:
                # 读取CSV为DataFrame
                df = pd.read_csv(csv_buffer, compression='gzip', low_memory=False)
                
                # 处理数据类型
                df = self._process_data_types(df)
                
                # 转换为Parquet并直接返回字节
                parquet_buffer = io.BytesIO()
                # 创建Schema，显式指定时间字段的类型
                schema = self._create_parquet_schema(df)
                table = pa.Table.from_pandas(df, schema=schema)
                
                # 直接使用PyArrow写入文件
                pq.write_table(
                    table,
                    parquet_buffer,
                    compression='snappy'
                )
                return parquet_buffer.getvalue()
        except Exception as e:
            logger.error(f"内存中处理CSV失败: {str(e)}")
            return None
    
    def _process_with_temp_file(self, csv_content: bytes) -> Optional[bytes]:
        """使用临时文件处理CSV到Parquet的转换
        
        Args:
            csv_content: CSV文件内容
            
        Returns:
            Parquet格式的字节内容
        """
        try:
            # 大文件使用临时文件处理
            with tempfile.NamedTemporaryFile(suffix='.csv.gz') as csv_tmp:
                csv_tmp.write(csv_content)
                csv_tmp.flush()
                
                # 读取CSV为DataFrame
                df = pd.read_csv(csv_tmp.name, compression='gzip', low_memory=False)
                
                # 处理数据类型
                df = self._process_data_types(df)
                
                # 转换为Parquet
                with tempfile.NamedTemporaryFile(suffix='.parquet') as parquet_tmp:
                    # 创建Schema，显式指定时间字段的类型
                    schema = self._create_parquet_schema(df)
                    table = pa.Table.from_pandas(df, schema=schema)
                    
                    # 直接使用PyArrow写入文件
                    pq.write_table(
                        table,
                        parquet_tmp.name,
                        compression='snappy'
                    )
                    with open(parquet_tmp.name, 'rb') as f:
                        return f.read()
        except Exception as e:
            logger.error(f"使用临时文件处理CSV失败: {str(e)}")
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
        
        # 处理时间字段
        for col in self.TIME_FIELDS:
            if col in df.columns:
                try:
                    # 转换为datetime类型
                    df[col] = pd.to_datetime(df[col])
                    # 移除时区信息
                    if df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)
                    # 不转换为字符串，保持timestamp类型
                    # 注意：这里不再转换为字符串，而是保持datetime类型
                    logger.debug(f"将字段 {col} 保持为timestamp类型")
                except Exception as e:
                    logger.error(f"Failed to convert {col}: {str(e)}")
        
        # 检查是否有 NaT 值
        for col in self.TIME_FIELDS:
            if col in df.columns:
                nat_count = df[col].isna().sum()
                if nat_count > 0:
                    logger.warning(f"Found {nat_count} NaT values in {col}")
        
        # 处理账号相关的字段，确保它们是字符串类型
        account_id_fields = [
            'bill_payer_account_id',
            'line_item_usage_account_id',
            'product_from_account_id',
            'product_to_account_id',
            'pricing_plan_arn',
            'resource_id'
        ]
        for col in account_id_fields:
            if col in df.columns:
                # 先将空值替换为空字符串，再转换为字符串类型
                df[col] = df[col].fillna('').astype(str)
        
        # 处理Map类型字段
        map_fields = [
            'cost_category',
            'discount',
            'product',
            'resource_tags'
        ]
        
        for field in map_fields:
            if field in df.columns:
                logger.debug(f"处理Map字段: {field}")
                df[field] = df[field].apply(lambda x: parse_json_or_default(x))
        
        return df
        
    def _create_parquet_schema(self, df: pd.DataFrame) -> pa.Schema:
        """创建Parquet Schema，正确处理特殊类型字段
        
        Args:
            df: 输入的DataFrame
            
        Returns:
            PyArrow Schema对象
        """
        # 创建字段列表
        fields = []
        
        # 处理所有列
        for col_name in df.columns:
            # 时间字段使用timestamp类型
            if col_name in self.TIME_FIELDS:
                fields.append(pa.field(col_name, pa.timestamp('ns')))
            
            # Map类型字段
            elif col_name == 'cost_category':
                fields.append(pa.field(col_name, pa.map_(pa.string(), pa.string())))
            elif col_name == 'discount':
                fields.append(pa.field(col_name, pa.map_(pa.string(), pa.float64())))
            elif col_name == 'product':
                fields.append(pa.field(col_name, pa.map_(pa.string(), pa.string())))
            elif col_name == 'resource_tags':
                fields.append(pa.field(col_name, pa.map_(pa.string(), pa.string())))
            
            # 字符串类型字段
            elif col_name in self.STRING_COLUMNS:
                fields.append(pa.field(col_name, pa.string()))
            
            # 其他字段使用自动推断类型
            else:
                # 获取pandas列的类型
                dtype = df[col_name].dtype
                
                # 根据pandas类型选择PyArrow类型
                if pd.api.types.is_integer_dtype(dtype):
                    fields.append(pa.field(col_name, pa.int64()))
                elif pd.api.types.is_float_dtype(dtype):
                    fields.append(pa.field(col_name, pa.float64()))
                elif pd.api.types.is_bool_dtype(dtype):
                    fields.append(pa.field(col_name, pa.bool_()))
                else:
                    # 默认使用字符串类型
                    fields.append(pa.field(col_name, pa.string()))
        
        return pa.schema(fields)


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
        
        # 确保结果是字典类型
        if not isinstance(result, dict):
            return {'_value': str(result)}
            
        # 确保字典不为空
        if not result:
            return {'_empty': ''}
            
        # 对于discount字段，确保值是浮点数
        for k, v in result.items():
            if isinstance(v, str) and v.replace('.', '', 1).isdigit():
                try:
                    result[k] = float(v)
                except:
                    pass
                    
        return result
    except Exception as e:
        logger.debug(f"JSON解析失败: {str(e)}, 值: {value}")
        return {'_error': str(value)}
