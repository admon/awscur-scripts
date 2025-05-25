import json
import hashlib
import logging
import os
import re
import sys
import tempfile
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import boto3
import botocore
import mysql.connector
import pandas as pd
from dotenv import load_dotenv

# 导入自定义模块
from parquet_utils import ParquetConverter

# 加载 .env 文件
load_dotenv()

logger = logging.getLogger(__name__)

# 工具函数
def normalize_s3_path(account: dict, key: str) -> str:
    """标准化S3路径，去除s3://前缀和bucket名
    
    Args:
        account: 账号信息，包含bucket字段
        key: S3对象键或完整路径
        
    Returns:
        标准化后的对象键
    """
    if key.startswith('s3://'):
        # 跳过 s3://bucket/ 前缀
        return key.split('/', 3)[3]
    elif account and 'bucket' in account and key.startswith(f"{account['bucket']}/"):
        # 跳过 bucket/ 前缀
        return key[len(account['bucket'])+1:]
    return key
    
def ensure_utc_timestamp(timestamp) -> datetime:
    """确保时间戳为UTC时区
    
    Args:
        timestamp: 输入时间戳
        
    Returns:
        带有UTC时区的时间戳
    """
    if timestamp is None:
        return None
    if isinstance(timestamp, (int, float)):
        # 如果是时间戳数值，转换为datetime
        timestamp = datetime.fromtimestamp(timestamp)
    if timestamp.tzinfo is None:
        # 如果没有时区信息，添加UTC时区
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp

class S3Handler:
    """处理S3相关操作"""
    def __init__(self):
        self.target_bucket = os.getenv('PARQUET_BUCKET')
        self.target_s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION', 'us-east-1'))
        self.s3_clients = {}
        
    def get_client(self, account: dict) -> boto3.client:
        """获取或创建S3客户端"""
        account_id = account['account_id']
        if account_id not in self.s3_clients:
            self.s3_clients[account_id] = boto3.client(
                's3',
                region_name=account['region_name'],
                aws_access_key_id=account['access_key_id'],
                aws_secret_access_key=account['secret_access_key']
            )
        return self.s3_clients[account_id]
        
    def get_object(self, account: dict, key: str) -> Optional[bytes]:
        """从S3获取对象内容"""
        try:
            s3_client = self.get_client(account)
            logger.debug(f"尝试读取S3对象: bucket={account['bucket']}, key={key}")
            response = s3_client.get_object(
                Bucket=account['bucket'],
                Key=key
            )
            return response['Body'].read()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.debug(f"文件不存在: bucket={account['bucket']}, key={key}")
            else:
                logger.error(f"获取S3对象失败: bucket={account['bucket']}, key={key}, 错误: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"获取S3对象失败: bucket={account['bucket']}, key={key}, 错误: {str(e)}")
            return None
            
    def get_object_metadata(self, account: dict, key: str) -> Optional[Dict]:
        """获取S3对象的元数据"""
        try:
            # 去掉可能存在的 s3:// 和 bucket 前缀
            if key.startswith('s3://'):
                key = key.split('/', 3)[3]  # 跳过 s3://bucket/
            elif key.startswith(f"{account['bucket']}/"):
                key = key[len(account['bucket'])+1:]
                
            s3_client = self.get_client(account)
            response = s3_client.head_object(
                Bucket=account['bucket'],
                Key=key
            )
            return {
                'last_modified': response['LastModified'],
                'size': response['ContentLength'],
                'etag': response['ETag']
            }
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ['NoSuchKey', '404']:
                logger.debug(f"文件不存在: bucket={account['bucket']}, key={key}")
            else:
                logger.error(f"获取S3对象元数据失败: bucket={account['bucket']}, key={key}, 错误: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"获取S3对象元数据失败: bucket={account['bucket']}, key={key}, 错误: {str(e)}")
            return None
            
    def check_object_exists(self, key: str) -> Tuple[bool, Optional[dict]]:
        """检查对象是否存在于目标S3
        
        Args:
            key: 对象键
            
        Returns:
            Tuple[bool, Optional[dict]]: (是否存在, 元数据信息)
        """
        try:
            response = self.target_s3_client.head_object(
                Bucket=self.target_bucket,
                Key=key
            )
            return True, {
                'last_modified': response['LastModified'],
                'size': response['ContentLength'],
                'etag': response['ETag']
            }
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False, None
            else:
                logger.error(f"检查文件存在失败: bucket={self.target_bucket}, key={key}, 错误: {str(e)}")
                return False, None
        except Exception as e:
            logger.error(f"检查文件存在失败: bucket={self.target_bucket}, key={key}, 错误: {str(e)}")
            return False, None
            
    def put_object(self, key: str, content: bytes) -> bool:
        """上传对象到目标S3"""
        try:
            logger.debug(f"开始上传文件到S3: bucket={self.target_bucket}, key={key}, size={len(content)} bytes")
            self.target_s3_client.put_object(
                Bucket=self.target_bucket,
                Key=key,
                Body=content
            )
            logger.debug(f"文件上传成功: bucket={self.target_bucket}, key={key}")
            return True
        except Exception as e:
            logger.error(f"上传S3对象失败: bucket={self.target_bucket}, key={key}, 错误: {str(e)}")
            return False

class DBHandler:
    """处理数据库相关操作"""
    def __init__(self):
        self.db = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        self.cursor = self.db.cursor(dictionary=True)
        
    def get_accounts(self) -> List[Dict]:
        """获取账号列表"""
        try:
            sql = """
            SELECT 
                account_id,
                account_name as name,
                region_name,
                access_key_id,
                secret_access_key,
                bucket,
                prefix,
                hourly_export,
                daily_export,
                monthly_export
            FROM exports
            WHERE bucket IS NOT NULL
            """
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"获取账号列表失败: {str(e)}")
            return []
            
    def get_processed_manifest(self, account_id: str, manifest_path: str) -> Optional[Dict]:
        """获取已处理的Manifest记录"""
        try:
            sql = """
            SELECT *
            FROM processedfiles
            WHERE account_id = %s AND manifest_path = %s
            """
            self.cursor.execute(sql, (account_id, manifest_path))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"获取处理记录失败: {str(e)}")
            return None
            
    def update_processed_manifest(self, **kwargs) -> bool:
        """更新Manifest处理状态"""
        # 重命名 account_id
        if 'aws_account_id' in kwargs:
            kwargs['account_id'] = kwargs.pop('aws_account_id')
            
        # 从 manifest_path 提取 manifest_partition
        manifest_path = kwargs.get('manifest_path', '')
        partition_match = re.search(r'BILLING_PERIOD=[^/]+', manifest_path)
        if partition_match:
            kwargs['manifest_partition'] = partition_match.group(0)
        else:
            logger.error(f"无法从 manifest_path 提取 manifest_partition: {manifest_path}")
            return False
            
        # 添加当前时间作为 processed_at
        kwargs['processed_at'] = datetime.now()
        
        # 记录要执行的参数
        logger.debug(f"更新处理状态参数: {kwargs}")
        
        # 确保 expected_files 字段存在且有效
        if 'expected_files' not in kwargs:
            logger.debug("expected_files 字段不存在，设置为空列表")
            kwargs['expected_files'] = '[]'
        elif isinstance(kwargs['expected_files'], str):
            try:
                # 尝试解析 JSON
                files = json.loads(kwargs['expected_files'])
                if not isinstance(files, list):
                    logger.error(f"expected_files 不是有效的文件名列表: {files}")
                    return False
                # 验证每个文件名格式
                for filename in files:
                    if not isinstance(filename, str) or not filename.endswith('.parquet'):
                        logger.error(f"无效的文件名格式: {filename}")
                        return False
                # 保持原有的 JSON 字符串
                logger.debug(f"有效的文件名列表: {files}")
            except json.JSONDecodeError:
                logger.error(f"无法解析 expected_files JSON: {kwargs['expected_files']}")
                return False
        else:
            logger.error(f"expected_files 类型无效: {type(kwargs['expected_files'])}")
            return False
        
        sql = """
        INSERT INTO processedfiles
            (account_id, manifest_path, manifest_partition, report_type, manifest_last_modified, status, error_message, processed_at, expected_files)
        VALUES
            (%(account_id)s, %(manifest_path)s, %(manifest_partition)s, %(report_type)s, %(manifest_last_modified)s, %(status)s, %(error_message)s, %(processed_at)s, %(expected_files)s)
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            error_message = VALUES(error_message),
            processed_at = VALUES(processed_at),
            manifest_last_modified = VALUES(manifest_last_modified),
            expected_files = VALUES(expected_files),
            updated_at = CURRENT_TIMESTAMP
        """
        try:
            self.cursor.execute(sql, kwargs)
            self.db.commit()
            return True
        except mysql.connector.Error as e:
            logger.error(f"数据库错误: {e.errno} - {e.msg}")
            return False
        except Exception as e:
            logger.error(f"更新处理状态失败: {str(e)}")
            return False

# ParquetConverter类已移动到parquet_utils.py模块中
                
class CURParquetConverter:
    """CUR文件转换器主类"""
    def __init__(self, memory_threshold_mb: int = 200):
        """初始化CUR文件转换器
        
        Args:
            memory_threshold_mb: 内存处理阈值（MB），小于此值的文件直接在内存中处理
        """
        self.s3_handler = S3Handler()
        self.db_handler = DBHandler()
        self.parquet_converter = ParquetConverter(memory_threshold_mb=memory_threshold_mb)
        self.tier_param = None  # 存储当前处理的tier参数
            
    def _extract_partition(self, manifest_path: str) -> str:
        """从路径中提取账期信息"""
        parts = manifest_path.split('/')
        billing_period = next(p for p in parts if p.startswith('BILLING_PERIOD='))
        return billing_period.split('=')[1]
        
    def _get_data_files_from_manifest(self, manifest: dict) -> List[str]:
        """从Manifest中提取数据文件列表
        
        处理字符串列表格式的dataFiles字段
        """
        try:
            # 首先检查manifest是否为字典类型
            if not isinstance(manifest, dict):
                logger.error(f"解析Manifest文件失败: manifest不是字典类型，而是{type(manifest)}")
                return []
                
            # 检查dataFiles是否存在且是列表类型
            data_files = manifest.get('dataFiles', [])
            if not isinstance(data_files, list):
                logger.error(f"解析Manifest文件失败: dataFiles不是列表类型，而是{type(data_files)}")
                return []
            
            # 如果列表为空，直接返回
            if not data_files:
                logger.warning("数据文件列表为空")
                return []
                
            # 直接处理字符串列表
            result = []
            for i, file_path in enumerate(data_files):
                if isinstance(file_path, str):
                    result.append(file_path)
                else:
                    # 如果是字典类型，尝试获取key字段
                    if isinstance(file_path, dict) and 'key' in file_path:
                        result.append(file_path['key'])
                    else:
                        logger.warning(f"跳过不支持的数据文件格式: 索引{i}, 类型{type(file_path)}")
            
            return result
            
        except Exception as e:
            logger.error(f"解析Manifest文件失败: {str(e)}")
            return []
            
    def _generate_target_filename(self, source_key: str) -> str:
        """生成目标文件名"""
        file_hash = hashlib.md5(source_key.encode()).hexdigest()[-16:]
        return f"{file_hash}.parquet"
        
    def _cleanup_old_files(self, account: dict, report_type: str, partition: str, expected_files: list) -> None:
        """清理不在预期文件列表中的旧文件"""
        try:
            # 构造要扫描的目录前缀，只扫描当前账期目录
            prefix = f"{report_type}/ID={account['account_id']}/cid-cur2/data/{partition}/"
            expected_files_set = set(expected_files)
            
            # 列出目标桶中的所有文件
            paginator = self.s3_handler.target_s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.s3_handler.target_bucket, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    # 提取文件名进行比较
                    filename = obj['Key'].split('/')[-1]
                    if filename not in expected_files_set:
                        logger.info(f"删除旧文件: {obj['Key']} (文件名: {filename})")
                        self.s3_handler.target_s3_client.delete_object(
                            Bucket=self.s3_handler.target_bucket,
                            Key=obj['Key']
                        )
                    else:
                        logger.debug(f"保留文件: {obj['Key']} (文件名: {filename})")
        except Exception as e:
            logger.error(f"清理旧文件失败: {str(e)}")

    def _get_target_key(self, account: dict, report_type: str, source_key: str) -> str:
        """根据账号信息、报告类型和源文件路径构造目标路径"""
        # 从路径中提取 BILLING_PERIOD
        parts = source_key.split('/')
        billing_period = next(p for p in parts if p.startswith('BILLING_PERIOD='))
        
        # 使用源文件路径的MD5值作为文件名
        file_hash = hashlib.md5(source_key.encode()).hexdigest()[-16:]
        target_filename = f"{file_hash}.parquet"
        
        # 构造目标路径
        target_key = f"{report_type}/ID={account['account_id']}/cid-cur2/data/{billing_period}/{target_filename}"
        
        # 记录源文件路径和生成的文件名
        logger.debug(f"源文件路径: {source_key} -> MD5: {file_hash}")
        return target_key
    

                
    def _get_processed_manifest(self, account_id: str, manifest_path: str) -> Optional[Dict[str, Any]]:
        """获取已处理的 Manifest 记录"""
        return self.db_handler.get_processed_manifest(account_id, manifest_path)
        
    def _update_processed_manifest(self, account_id: str, manifest_path: str, 
                                report_type: str, manifest_last_modified: datetime,
                                status: str, error_message: Optional[str] = None,
                                expected_files: Optional[List[str]] = None):
        """更新 Manifest 处理状态
        
        Args:
            account_id: 账号ID
            manifest_path: Manifest文件路径
            report_type: 报告类型
            manifest_last_modified: Manifest文件最后修改时间
            status: 处理状态
            error_message: 错误信息
            expected_files: 预期的Parquet文件名列表
        """
        # 将expected_files列表转换为JSON字符串
        expected_files_json = '[]'
        if expected_files:
            try:
                expected_files_json = json.dumps(expected_files)
                logger.debug(f"预期文件列表: {expected_files_json}")
            except Exception as e:
                logger.error(f"转换expected_files为JSON失败: {str(e)}")
                
        return self.db_handler.update_processed_manifest(
            account_id=account_id,
            manifest_path=manifest_path,
            report_type=report_type,
            manifest_last_modified=manifest_last_modified,
            status=status,
            error_message=error_message,
            expected_files=expected_files_json
        )
    
    def _get_s3_client(self, account):
        """获取或创建账号的S3客户端"""
        return self.s3_handler.get_client(account)

    def _read_gzip_to_df(self, account: dict, key: str) -> pd.DataFrame:
        """从S3读取gzip格式的CSV文件并转换为DataFrame"""
        content = self.s3_handler.get_object(account, key)
        if not content:
            raise Exception(f"无法读取文件: {key}")
            
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            return pd.read_csv(gz, low_memory=False)

    def _get_schema_from_df(self, df: pd.DataFrame) -> List[Dict]:
        """从 DataFrame 推断 schema，保留所有字段"""
        columns = []
        for col_name, dtype in df.dtypes.items():
            # 根据 pandas 类型推断 Athena 类型
            if dtype == 'datetime64[ns]':
                col_type = 'timestamp'
            elif dtype == 'float64':
                col_type = 'double'
            elif dtype == 'int64' or dtype == 'int32':
                # 检查是否是账号相关字段
                if any(id_field in col_name.lower() for id_field in ['account', 'arn', 'id']):
                    col_type = 'string'
                else:
                    col_type = 'bigint'
            else:
                col_type = 'string'
            
            columns.append({
                'Name': col_name,
                'Type': col_type
            })
        
        return columns

    def _standardize_cur_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化 CUR 数据格式
        处理四个时间字段和账号 ID 字段，其他字段保持原样
        """
        # 需要处理的时间字段
        time_fields = [
            'line_item_usage_start_date',
            'line_item_usage_end_date',
            'bill_billing_period_start_date',
            'bill_billing_period_end_date'
        ]
        
        # 处理时间字段
        for col in time_fields:
            if col in df.columns:
                try:
                    # 先转换为 datetime
                    df[col] = pd.to_datetime(df[col])
                    # 移除时区信息
                    if df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)
                    # 转换为字符串格式 YYYY-MM-DD HH:MM:SS.000
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
                except Exception as e:
                    logger.error(f"Failed to convert {col}: {str(e)}")
        
        # 检查是否有 NaT 值
        for col in time_fields:
            if col in df.columns:
                nat_count = df[col].isna().sum()
                if nat_count > 0:
                    logger.warning(f"Found {nat_count} NaT values in {col}")
        
        # 处理账号相关的字段，确保它们是字符串类型
        account_id_fields = [
            'bill_payer_account_id',
            'line_item_usage_account_id',
            'product_from_account_id',  # 添加其他可能的账号字段
            'product_to_account_id',
            'pricing_plan_arn',  # ARN 也包含账号信息
            'resource_id'  # 资源ID可能包含账号
        ]
        for col in account_id_fields:
            if col in df.columns:
                # 先将空值替换为空字符串，再转换为字符串类型
                df[col] = df[col].fillna('').astype(str)
        
        return df



    def _clean_old_parquet_files(self, target_prefix: str, new_file_key: str) -> bool:
        """清理指定目录下的旧 Parquet 文件
        
        Args:
            target_prefix: 目标目录
            new_file_key: 新文件的完整路径
            
        Returns:
            bool: 如果目标文件已存在且不需要更新，返回 True
        """
        try:
            # 检查新文件是否已存在
            exists, metadata = self.s3_handler.check_object_exists(new_file_key)
            last_modified = metadata['last_modified'] if metadata else None
            if exists:
                # 如果文件存在且修改时间在24小时内，不需要更新
                age = datetime.now(timezone.utc) - last_modified
                if age < timedelta(hours=24):
                    logger.debug(f"文件 {new_file_key} 已存在且修改时间在24小时内，跳过处理")
                    return True

            # 列出目录下的所有 Parquet 文件
            response = self.s3_handler.target_s3_client.list_objects_v2(
                Bucket=self.s3_handler.target_bucket,
                Prefix=target_prefix
            )
            
            # 收集要删除的文件
            current_time = datetime.now(timezone.utc)
            objects_to_delete = []
            
            for obj in response.get('Contents', []):
                if not obj['Key'].endswith('.parquet'):
                    continue
                    
                # 检查文件年龄
                age = current_time - obj['LastModified']
                if age < timedelta(hours=24):
                    logger.debug(f"跳过文件 {obj['Key']}，存在时间不足24小时")
                    continue
                    
                objects_to_delete.append({'Key': obj['Key']})
            
            if objects_to_delete:
                logger.debug(f"清理目录 {target_prefix} 下的 {len(objects_to_delete)} 个超过24小时的旧文件")
                # 批量删除文件
                self.s3_handler.target_s3_client.delete_objects(
                    Bucket=self.s3_handler.target_bucket,
                    Delete={'Objects': objects_to_delete}
                )
            
            return False
            
        except Exception as e:
            logger.error(f"清理旧文件失败: {target_prefix}, 错误: {str(e)}")
            raise

    def _upload_parquet_to_s3(self, df: pd.DataFrame, key: str, clean_old: bool = True):
        """将DataFrame上传为Parquet格式到S3"""
        try:
            # 如果需要清理旧文件并检查文件是否需要更新
            if clean_old:
                # 获取目标目录
                target_prefix = os.path.dirname(key)
                if 'BILLING_PERIOD=' in target_prefix:
                    # 清理同一账期目录下的旧文件，并检查是否需要更新
                    if self._clean_old_parquet_files(target_prefix, key):
                        logger.info(f"跳过文件 {key} 的处理")
                        return
            
            # 创建临时文件
            with tempfile.NamedTemporaryFile() as tmp:
                # 写入Parquet文件
                df.to_parquet(tmp.name, index=False)
                
                # 读取文件内容
                with open(tmp.name, 'rb') as f:
                    content = f.read()
                
                # 上传到S3
                return self.s3_handler.put_object(key, content)
        except Exception as e:
            logger.error(f"上传文件失败: {key}, 错误: {str(e)}")
            raise

    def _process_manifest(self, account: dict, manifest_path: str, report_type: str, force: bool = False) -> bool:
        """处理单个Manifest文件
        
        Args:
            account: 账号信息
            manifest_path: Manifest文件路径
            report_type: 报告类型
            force: 是否强制处理
            
        Returns:
            bool: 处理是否成功
        """
        try:
            # 获取manifest文件元数据
            manifest_metadata = self.s3_handler.get_object_metadata(account, manifest_path)
            if not manifest_metadata:
                logger.error(f"Manifest文件不存在: {manifest_path}")
                return False
                
            manifest_last_modified = manifest_metadata['last_modified']
            
            # 检查是否需要处理
            if not force:
                processed = self._get_processed_manifest(account['account_id'], manifest_path)
                if processed and processed['status'] == 'success':
                    # 处理时间戳的时区问题
                    db_last_modified = ensure_utc_timestamp(processed['manifest_last_modified'])
                    s3_last_modified = ensure_utc_timestamp(manifest_last_modified)
                    
                    # 比较时间戳
                    if abs((db_last_modified - s3_last_modified).total_seconds()) < 1:
                        logger.info(f"Manifest文件未变化，跳过处理: {manifest_path}")
                        return True
                    else:
                        logger.info(f"Manifest文件已更新，需要重新处理: {manifest_path}")
            
            # 读取manifest文件
            manifest = self._read_manifest_file(account, manifest_path)
            if not manifest:
                logger.error(f"读取Manifest文件失败: {manifest_path}")
                return False
                
            # 提取账期信息
            partition = self._extract_partition(manifest_path)
            
            # 获取需要处理的文件列表
            data_files = self._get_data_files_from_manifest(manifest)
            if not data_files:
                logger.warning(f"Manifest文件中没有数据文件: {manifest_path}")
                return False
                
            # 生成预期文件列表
            expected_files = [self._generate_target_filename(f) for f in data_files]
            
            # 处理每个数据文件
            success = True
            for source_key in data_files:
                target_key = self._get_target_key(account, report_type, source_key)
                if not self._process_data_file(account, source_key, target_key):
                    success = False
                    
            # 清理旧文件
            self._cleanup_old_files(account, report_type, partition, expected_files)
            
            # 更新处理状态
            self._update_processed_manifest(
                account['account_id'],
                manifest_path,
                report_type,
                manifest_last_modified,
                'success' if success else 'error',
                None,  # error_message
                expected_files  # 传递预期文件列表
            )
            
            return success
            
        except Exception as e:
            logger.error(f"处理Manifest文件失败: {manifest_path}, 错误: {str(e)}")
            return False
            
    def _process_account(self, account: dict, path_filter: Optional[str] = None, force: bool = False) -> bool:
        """处理单个账号的CUR文件
        
        Args:
            account: 账号信息
            path_filter: 路径过滤器
            force: 是否强制处理
            
        Returns:
            bool: 处理是否成功
        """
        try:
            logger.info(f"处理账号: {account['name']} ({account['account_id']})")
            
            # 确定要处理的报告类型
            report_types = []
            report_configs = [
                ('hourly', account.get('hourly_export')),
                ('daily', account.get('daily_export')),
                ('monthly', account.get('monthly_export'))
            ]
            
            # 根据tier过滤报告类型
            for report_tier, report_name in report_configs:
                # 如果指定了tier，只处理该tier的报告
                if path_filter and path_filter.startswith('tier=') and path_filter[5:] != report_tier:
                    continue
                    
                # 如果指定了tier参数，只处理该tier的报告
                tier_param = self.tier_param
                if tier_param and tier_param != report_tier:
                    continue
                    
                if report_name:  # 只处理配置了报告名称的报告
                    report_types.append((report_tier, report_name))
            
            if not report_types:
                logger.warning(f"账号未配置任何报告: {account['account_id']}")
                return True  # 没有报告也算成功
            
            success = True
            for report_tier, report_name in report_types:
                logger.info(f"处理报告: {report_name} (类型: {report_tier})")
                
                # 获取账期目录
                billing_periods = self._list_billing_periods(account, report_name, path_filter)
                if not billing_periods:
                    logger.warning(f"未找到账期目录: {report_name}")
                    continue
                
                # 处理每个账期的Manifest文件
                for partition in billing_periods:
                    manifest_key = self._get_manifest_key(account, report_name, partition)
                    if not manifest_key:
                        continue
                        
                    # 处理manifest文件
                    if not self._process_manifest(account, manifest_key, report_tier, force):
                        success = False
            
            return success
            
        except Exception as e:
            logger.error(f"处理账号失败: {account['account_id']}, 错误: {str(e)}")
            return False
            
    def _get_filtered_accounts(self, payer_id: Optional[str] = None, tier: Optional[str] = None) -> List[dict]:
        """获取符合条件的账号列表
        
        Args:
            payer_id: 付款账号ID
            tier: 账号层级
            
        Returns:
            符合条件的账号列表
        """
        accounts = self.db_handler.get_accounts()
        if payer_id:
            accounts = [a for a in accounts if a['account_id'] == payer_id]
        if tier:
            accounts = [a for a in accounts if a.get(f'{tier}_export')]
        return accounts
        
    def _get_accounts(self) -> List[Dict]:
        """从数据库获取账号信息"""
        return self.db_handler.get_accounts()

    def _read_manifest_file(self, account: dict, key: str) -> Dict:
        """从S3读取Manifest文件
        
        Args:
            account: 账号信息
            key: manifest文件路径，可能包含bucket名
            
        Returns:
            manifest文件内容，包含数据文件列表和其他元数据
        """
        # 保存原始路径用于错误日志
        original_key = key
        
        # 如果key包含bucket，需要去掉bucket部分
        if account.get('bucket') and key.startswith(f"{account['bucket']}/"):
            key = key[len(account['bucket'])+1:]
            
        # 尝试读取manifest文件
        content = self.s3_handler.get_object(account, key)
        if not content:
            logger.warning(f"未找到manifest文件: {original_key}")
            return {}
            
        try:
            manifest = json.loads(content)
            
            # 检查manifest文件格式
            if not isinstance(manifest, dict):
                logger.error(f"Manifest文件格式错误，应为JSON对象: {original_key}")
                return {}
                
            # 检查是否包含必要的字段
            if 'dataFiles' not in manifest:
                logger.error(f"Manifest文件缺少dataFiles字段: {original_key}")
                return {}
                
            return manifest
            
        except json.JSONDecodeError as e:
            logger.error(f"Manifest文件格式错误: {original_key}, 错误: {str(e)}")
            return {}

    def _process_data_file(self, account: dict, source_key: str, target_key: str) -> bool:
        """处理单个数据文件"""
        try:
            # 标准化源文件路径
            source_key = normalize_s3_path(account, source_key)
                
            # 获取源文件
            logger.info(f"开始处理数据文件: {source_key}")
            content = self.s3_handler.get_object(account, source_key)
            if not content:
                logger.error(f"获取源文件失败: {source_key}")
                return False
            
            # 如果源文件是Parquet格式，直接使用
            if source_key.lower().endswith('.parquet'):
                logger.info(f"源文件已经是Parquet格式，直接使用: {source_key}")
                parquet_content = content
            else:
                # 转换为Parquet格式
                logger.info(f"开始转换为Parquet格式: {source_key}, size={len(content)} bytes")
                parquet_content = self.parquet_converter.convert_csv_to_parquet(content)
                if not parquet_content:
                    logger.error(f"转换为Parquet格式失败: {source_key}")
                    return False
            
            # 上传到目标位置
            logger.info(f"开始上传Parquet文件: {target_key}, size={len(parquet_content)} bytes")
            result = self.s3_handler.put_object(target_key, parquet_content)
            if result:
                logger.info(f"文件处理成功: {source_key} -> {target_key}")
            return result
        except Exception as e:
            logger.error(f"处理文件失败: {source_key}, 错误: {str(e)}")
            return False

    def _list_billing_periods(self, account: dict, report_name: str, path_filter: Optional[str] = None) -> List[str]:
        """获取账期目录列表
        
        Args:
            account: 账号信息
            report_name: 报告名称
            path_filter: 路径过滤器，必须以 'BILLING_PERIOD=' 开头
            
        Returns:
            账期目录列表
        """
        try:
            # 构造报告路径
            metadata_prefix = f"{account['prefix']}/{report_name}/metadata/"
            logger.debug(f"查找账期目录: bucket={account['bucket']}, prefix={metadata_prefix}")
            
            # 如果有路径过滤器，直接返回该账期
            if path_filter:
                logger.debug(f"使用路径过滤器: {path_filter}")
                return [path_filter]
            
            # 列出 metadata 目录下的所有账期目录
            client = self.s3_handler.get_client(account)
            logger.debug(f"得到S3客户端: {client._endpoint.host}")
            
            response = client.list_objects_v2(
                Bucket=account['bucket'],
                Prefix=metadata_prefix,
                Delimiter='/'
            )
            
            # 记录响应信息
            logger.debug(f"S3响应: {json.dumps(response, default=str)}")
            
            # 提取账期目录
            billing_periods = []
            for prefix in response.get('CommonPrefixes', []):
                # 从路径中提取账期
                path = prefix['Prefix']
                match = re.search(r'BILLING_PERIOD=([^/]+)', path)
                if match:
                    period = match.group(1)
                    # 只过滤未来的账期
                    current_date = datetime.now()
                    current_period = current_date.strftime('%Y-%m')
                    if period <= current_period:
                        billing_periods.append(f"BILLING_PERIOD={period}")
                    
            logger.debug(f"找到账期目录: {billing_periods}")
            return sorted(billing_periods, reverse=True)
            
        except Exception as e:
            logger.error(f"获取账期目录失败: {report_name}, 错误: {str(e)}")
            return []
            
    def _get_manifest_key(self, account: dict, report_name: str, partition: str) -> str:
        """构造Manifest文件路径
        
        Args:
            account: 账号信息
            report_name: 报告名称
            partition: 账期目录
            
        Returns:
            Manifest文件路径
        """
        # 构造不带bucket前缀的路径
        manifest_path = f"{account['prefix']}/{report_name}/metadata/{partition}/{report_name}-Manifest.json"
        return manifest_path
    
    def process_cur_files(self, full_sync=False, payer_id=None, tier=None,
                         path_filter=None, force=False) -> bool:
        """处理CUR文件
        
        Args:
            full_sync: 是否全量同步
            payer_id: 付款账号ID
            tier: 账号层级
            path_filter: 路径过滤器
            force: 是否强制处理
            
        Returns:
            bool: 处理是否成功
        """
        try:
            # 设置tier参数，供其他方法使用
            self.tier_param = tier
            logger.info(f"开始处理CUR文件: tier={tier}, path_filter={path_filter}, force={force}")
            
            # 获取需要处理的账号
            accounts = self._get_filtered_accounts(payer_id, tier)
            if not accounts:
                logger.error("未找到符合条件的账号")
                return False
                
            success = True
            for account in accounts:
                if not self._process_account(account, path_filter, force):
                    success = False
                    
            return success
            
        except Exception as e:
            logger.error(f"处理CUR文件失败: {str(e)}")
            return False

def main():
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='转换 CUR 文件到 Parquet 格式')
    parser.add_argument('--debug', action='store_true', help='开启调试日志')
    parser.add_argument('--full', action='store_true', help='全量同步')
    parser.add_argument('--force', action='store_true', help='强制重新处理所有文件，即使之前处理成功')
    parser.add_argument('--payer', help='指定付费账号')
    parser.add_argument('--tier', choices=['hourly', 'daily', 'monthly'], help='指定账单粒度')
    parser.add_argument('--path', help='指定路径过滤器')
    parser.add_argument('--memory-threshold', type=int, default=200, help='内存处理阈值（MB），小于此值的文件直接在内存中处理，默认为200MB')
    
    args = parser.parse_args()
    
    # 检查分区路径格式
    if args.path and not args.path.startswith('BILLING_PERIOD='):
        parser.error("--path 参数必须以 'BILLING_PERIOD=' 开头")

    # 设置日志
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 关闭 botocore 的调试日志
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # 初始化转换器
    converter = CURParquetConverter(memory_threshold_mb=args.memory_threshold)
    success = converter.process_cur_files(
        full_sync=args.full,
        payer_id=args.payer,
        tier=args.tier,
        path_filter=args.path,
        force=args.force
    )
    
    # 如果有文件处理失败，以非零状态码退出
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()