import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import logging
from datetime import datetime
import gzip
import json
import io
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
from dotenv import load_dotenv
import mysql.connector

# 加载 .env 文件
load_dotenv()

logger = logging.getLogger(__name__)

class CURParquetConverter:
    def __init__(self):
        # 获取AWS区域配置
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        # 创建AWS客户端，统一使用相同的region
        self.s3_client = boto3.client('s3', region_name=self.region)
        
        # 获取存储桶配置
        self.source_bucket = os.getenv('BACKUP_BUCKET')
        self.target_bucket = os.getenv('PARQUET_BUCKET')
        
    def _read_gzip_to_df(self, bucket: str, key: str) -> pd.DataFrame:
        """从S3读取gzip格式的CSV文件并转换为DataFrame"""
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        with gzip.GzipFile(fileobj=response['Body']) as gz:
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
            #elif dtype == 'int64':
            #    col_type = 'bigint'
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
        
        # 处理账号 ID 字段，确保它们是字符串类型
        account_id_fields = [
            'bill_payer_account_id',
            'line_item_usage_account_id'
        ]
        for col in account_id_fields:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        return df

    def _upload_parquet_to_s3(self, df: pd.DataFrame, target_key: str):
        """将DataFrame保存为Parquet格式并上传到S3"""
        try:
            with io.BytesIO() as buffer:
                # 直接转换为 parquet 格式，保留所有字段
                table = pa.Table.from_pandas(df)
                pq.write_table(table, buffer)
                
                # 上传到S3
                buffer.seek(0)
                self.s3_client.put_object(
                    Bucket=self.target_bucket,
                    Key=target_key,
                    Body=buffer.getvalue()
                )
                logger.info(f"成功上传文件到: s3://{self.target_bucket}/{target_key}")
        except Exception as e:
            logger.error(f"上传文件失败: {target_key}, 错误: {str(e)}")
            raise



    def get_payer_accounts(self):
        """从数据库获取所有AWS账号信息"""
        try:
            # 从 .env 文件读取数据库配置
            db_config = {
                'host': os.getenv('DB_HOST'),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'database': os.getenv('DB_NAME')
            }
            
            # 检查必要的环境变量是否存在
            missing_vars = [k for k, v in db_config.items() if v is None]
            if missing_vars:
                raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
                
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM payer order by aws_id")
            accounts = cursor.fetchall()
            cursor.close()
            conn.close()
            return accounts
        except Exception as e:
            logger.error(f"获取账号信息失败: {str(e)}")
            raise



    def _read_manifest_file(self, bucket: str, key: str) -> dict:
        """读取并解析 Manifest 文件"""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            manifest_content = json.loads(response['Body'].read().decode('utf-8'))
            return manifest_content
        except Exception as e:
            logger.error(f"读取 Manifest 文件失败: {key}, 错误: {str(e)}")
            raise

    def _list_manifest_files(self, bucket: str, prefix: str) -> list:
        """列出目录中的 Manifest 文件
        只处理指定 BILLING_PERIOD 目录下的 Manifest 文件，忽略子目录中的文件
        """
        try:
            manifests = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('-Manifest.json'):
                        manifest_key = obj['Key']
                        # 检查文件是否在当前目录下，而不是在子目录中
                        if manifest_key.count('/') == prefix.count('/') + 1:
                            manifests.append(manifest_key)
            return manifests
        except Exception as e:
            logger.error(f"列出 Manifest 文件失败: {prefix}, 错误: {str(e)}")
            raise

    def process_cur_files(self, full_sync=False, hours=24, payer_id=None, tier=None, path_filter=None):
        """处理CUR文件的主函数
        Args:
            full_sync (bool): 是否执行全量同步
            hours (int): 同步指定小时数内的变动，当 full_sync 为 True 时忽略
            payer_id (str): 指定要同步的 Payer Account ID
            tier (str): 指定要处理的数据粒度（hourly/daily/monthly）
        """
        accounts = self.get_payer_accounts()
        
        # 如果指定了payer，只处理指定的账号
        if payer_id:
            accounts = [acc for acc in accounts if acc['aws_id'] == payer_id]
            if not accounts:
                logger.error(f"找不到 Payer Account ID: {payer_id}")
                return

        for account in accounts:
            logger.info(f"处理账号: {account['name']} ({account['aws_id']})")
            
            # 定义报告粒度
            report_types = ['monthly', 'daily', 'hourly'] if not tier else [tier]

            # 遍历所有报告粒度
            for report_type in report_types:
                # 构造 Manifest 文件路径
                if path_filter:
                    billing_period = path_filter.split('=')[1]
                    manifest_dir = (
                        f"{report_type}/"
                        f"{account['aws_id']}/"
                        f"metadata/"
                        f"BILLING_PERIOD={billing_period}"
                    )
                else:
                    manifest_dir = f"{report_type}/{account['aws_id']}/metadata"

                logger.info(f"扫描 Manifest 目录: {manifest_dir}")

                # 列出 metadata 目录下的所有子目录
                manifests = []
                paginator = self.s3_client.get_paginator('list_objects_v2')
                
                # 先获取 metadata 目录
                metadata_response = self.s3_client.list_objects_v2(
                    Bucket=self.source_bucket,
                    Prefix=manifest_dir,
                    Delimiter='/'
                )
                
                # 遍历并检查 BILLING_PERIOD= 目录
                for billing_prefix in billing_response.get('CommonPrefixes', []):
                    prefix_path = billing_prefix.get('Prefix')
                    logger.info(f"找到子目录: {prefix_path}")
                    # 检查是否是 BILLING_PERIOD= 格式的目录
                    if 'BILLING_PERIOD=' in prefix_path:
                        manifests.append(prefix_path)
                        logger.info(f"添加符合条件的子目录: {prefix_path}")
            
            if not manifests:
                logger.warning(f"未找到子目录: {manifest_dir}")
                continue
            
            # 遍历每个子目录，获取其中的 Manifest 文件
            for manifest_dir in manifests:
                logger.info(f"处理子目录: {manifest_dir}")
                # 只获取子目录下的 Manifest 文件，不获取更深层的目录
                for page in paginator.paginate(Bucket=self.source_bucket, Prefix=manifest_dir, Delimiter='/'):
                    for obj in page.get('Contents', []):
                        if obj['Key'].endswith('-Manifest.json'):
                            manifest_key = obj['Key']
                            # 检查文件是否在当前目录下
                            if manifest_key.startswith(manifest_dir):
                                logger.info(f"找到 Manifest 文件: {manifest_key}")
                                manifests.append(manifest_key)
            
            if not manifests:
                logger.warning(f"未找到 Manifest 文件: {manifest_dir}")
                continue

            for manifest_key in manifests:
                try:
                    # 读取 Manifest 文件
                    manifest_content = self._read_manifest_file(self.source_bucket, manifest_key)
                    logger.info(f"成功读取 Manifest 文件: {manifest_key}")
                    
                    # 获取数据文件列表
                    data_files = manifest_content.get('dataFiles', [])
                    logger.info(f"数据文件列表大小: {len(data_files)}")
                    
                    # 只处理最新的数据文件
                    latest_data_file = max(data_files, key=lambda x: x['compression'])
                    logger.info(f"处理最新的数据文件: {latest_data_file['key']}")
                    
                    # 标准化数据格式
                    logger.info(f"标准化数据格式: {latest_data_file['key']}")
                    df = self._standardize_cur_data(pd.read_csv(latest_data_file['key']))
                    
                    # 检查DataFrame是否为空
                    if df.empty:
                        logger.warning(f"跳过空的DataFrame: {latest_data_file['key']}")
                        continue

                    # 使用源文件的完整路径计算MD5
                    import hashlib
                    file_hash = hashlib.md5(latest_data_file['key'].encode()).hexdigest()[-16:]
                    
                    # 从源文件路径中提取账单周期
                    billing_period = None
                    for part in latest_data_file['key'].split('/'):
                        if part.startswith('BILLING_PERIOD='):
                            billing_period = part.split('=')[1]
                            break
                    
                    if not billing_period:
                        logger.warning(f"无法从文件路径中提取账单周期: {latest_data_file['key']}")
                        if path_filter and path_filter.startswith('BILLING_PERIOD='):
                            billing_period = path_filter.split('=')[1]
                        else:
                            continue
                    
                    # 构造目标文件路径
                    target_key = (
                        f"{report_type}/"
                        f"ID={account['aws_id']}/"
                        f"cid-cur2/"
                        f"data/"
                        f"BILLING_PERIOD={billing_period}/"
                        f"{file_hash}.parquet"
                    )
                    
                    # 上传Parquet文件
                    logger.info(f"开始上传Parquet文件: {target_key}")
                    self._upload_parquet_to_s3(df, target_key)
                    
                    logger.info(f"文件处理完成: {latest_data_file['key']}")
                except Exception as e:
                    logger.error(f"处理数据文件失败: {manifest_key}, 错误: {str(e)}")

def main():
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='转换 CUR 文件到 Parquet 格式')
    parser.add_argument('--full', action='store_true',
                      help='执行全量同步')
    parser.add_argument('--hour', type=int,
                      help='同步指定小时数内的变动，默认24小时')
    parser.add_argument('--payer', type=str,
                      help='指定要同步的 Payer Account ID')
    parser.add_argument('--tier', type=str, choices=['hourly', 'daily', 'monthly'],
                      help='指定要同步的数据粒度，不指定时处理所有粒度')
    parser.add_argument('--path', type=str,
                      help='指定要同步的分区路径，例如：BILLING_PERIOD=2025-01')

    args = parser.parse_args()
    
    # 检查参数冲突
    if args.full and args.hour is not None:
        parser.error("--full 和 --hour 参数不能同时使用")
        
    # 检查分区路径格式
    if args.path:
        if not args.path.startswith('BILLING_PERIOD='):
            parser.error("--path 参数必须以 'BILLING_PERIOD=' 开头")

    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 设置同步小时数
    hours = None if args.full else (args.hour or 24)
    
    converter = CURParquetConverter()
    converter.process_cur_files(full_sync=args.full, hours=hours, payer_id=args.payer, tier=args.tier, path_filter=args.path)

if __name__ == "__main__":
    main()
