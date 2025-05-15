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
        with io.BytesIO() as buffer:
            # 标准化数据格式（只处理时间字段，其他字段保持原样）
            df = self._standardize_cur_data(df)
            
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



    def process_cur_files(self, full_sync=False, hours=24, payer_id=None, tier=None):
        """处理CUR文件的主函数
        Args:
            full_sync (bool): 是否执行全量同步
            hours (int): 同步指定小时数内的变动，当 full_sync 为 True 时忽略
            payer_id (str): 指定要同步的 Payer Account ID
            tier (str): 指定要处理的数据粒度（hourly/daily/monthly）
        """
        accounts = self.get_payer_accounts()
        
        # 如果指定了 payer_id，只处理该账号
        if payer_id:
            accounts = [acc for acc in accounts if acc['aws_id'] == payer_id]
            if not accounts:
                logger.error(f"No account found with AWS ID: {payer_id}")
                return
        
        # 计算指定小时数前的时间戳
        if not full_sync:
            from datetime import datetime, timedelta
            time_threshold = (datetime.now() - timedelta(hours=hours)).timestamp()
            
        for account in accounts:
            logger.info(f"处理账号: {account['name']} ({account['aws_id']})")
            
            # 如果指定了粒度，只处理指定粒度的数据
            granularities = [tier] if tier else ['hourly', 'daily', 'monthly']
            for granularity in granularities:
                # 记录是否需要更新分区
                needs_partition_update = False
                
                # 列出该粒度下的所有文件
                paginator = self.s3_client.get_paginator('list_objects_v2')
                prefix = f"{granularity}/{account['aws_id']}/"
                
                for page in paginator.paginate(Bucket=self.source_bucket, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        source_key = obj['Key']
                        if not source_key.endswith('.csv.gz'):
                            continue
                            
                        # 如果不是全量同步，检查文件的最后修改时间
                        if not full_sync:
                            if obj['LastModified'].timestamp() < time_threshold:
                                logger.debug(f"Skipping old file: {source_key} (LastModified: {obj['LastModified']})")
                                continue
                        
                        logger.info(f"Processing file: {source_key} (LastModified: {obj['LastModified']})")

                        try:
                            # 从路径中提取日期信息
                            billing_period = next(
                                part.split('=')[1]
                                for part in source_key.split('/')
                                if part.startswith('BILLING_PERIOD=')
                            )
                            
                            # 使用源文件的完整路径计算MD5
                            import hashlib
                            file_hash = hashlib.md5(source_key.encode()).hexdigest()[-16:]
                            # 读取并转换数据
                            df = self._read_gzip_to_df(self.source_bucket, source_key)
                            
                            # 构造目标路径
                            target_key = (
                                f"{granularity}/"
                                f"ID={account['aws_id']}/"
                                f"cid-cur2/"
                                f"data/"
                                f"BILLING_PERIOD={billing_period}/"
                                f"{file_hash}.parquet"
                            )
                            

                            # 上传parquet文件
                            self._upload_parquet_to_s3(df, target_key)
                            logger.info(f"Converted {source_key} to {target_key}")
                            needs_partition_update = True
                            
                        except Exception as e:
                            logger.error(f"Error processing {source_key}: {str(e)}")
                            continue
                
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

    args = parser.parse_args()
    
    # 检查参数冲突
    if args.full and args.hour is not None:
        parser.error("--full 和 --hour 参数不能同时使用")

    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 设置同步小时数
    hours = None if args.full else (args.hour or 24)
    
    converter = CURParquetConverter()
    converter.process_cur_files(full_sync=args.full, hours=hours, payer_id=args.payer, tier=args.tier)

if __name__ == "__main__":
    main()
