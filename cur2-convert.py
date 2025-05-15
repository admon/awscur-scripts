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
        self.athena_client = boto3.client('athena', region_name=self.region)
        self.glue_client = boto3.client('glue', region_name=self.region)
        
        # 获取存储桶和数据库配置
        self.source_bucket = os.getenv('BACKUP_BUCKET')
        self.target_bucket = os.getenv('PARQUET_BUCKET')
        self.database_name = os.getenv('ATHENA_DATABASE', 'cur_analysis')
        
    def _read_gzip_to_df(self, bucket: str, key: str) -> pd.DataFrame:
        """从S3读取gzip格式的CSV文件并转换为DataFrame"""
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        with gzip.GzipFile(fileobj=response['Body']) as gz:
            return pd.read_csv(gz, low_memory=False)

    def _get_cur_schema(self) -> pa.Schema:
        """获取 CUR 数据的标准 schema 定义"""
        return pa.schema([
            # 时间戳字段 - 使用无时区的毫秒级时间戳
            ('line_item_usage_start_date', pa.timestamp('ms', tz=None)),
            ('line_item_usage_end_date', pa.timestamp('ms', tz=None)),
            ('pricing_public_on_demand_rate', pa.float64()),
            ('pricing_public_on_demand_cost', pa.float64()),
            ('line_item_unblended_rate', pa.float64()),
            ('line_item_unblended_cost', pa.float64()),
            # 其他字段可以根据需要添加
        ])

    def _standardize_cur_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化 CUR 数据格式"""
        # 处理时间戳字段 - 确保时区一致性
        time_columns = [
            'line_item_usage_start_date',
            'line_item_usage_end_date'
        ]
        
        for col in time_columns:
            if col in df.columns:
                # 将时间字符串转换为 datetime 对象
                df[col] = pd.to_datetime(df[col])
                # 如果有时区信息，转换到 UTC 然后移除时区信息
                if df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None)
        
        # 处理数值字段 - 确保类型一致性
        numeric_columns = [
            'pricing_public_on_demand_rate',
            'pricing_public_on_demand_cost',
            'line_item_unblended_rate',
            'line_item_unblended_cost'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    def _upload_parquet_to_s3(self, df: pd.DataFrame, target_key: str):
        """将DataFrame保存为Parquet格式并上传到S3"""
        with io.BytesIO() as buffer:
            # 标准化数据格式
            df = self._standardize_cur_data(df)
            
            # 使用预定义的 schema 转换为 parquet 格式
            schema = self._get_cur_schema()
            table = pa.Table.from_pandas(df, schema=schema)
            pq.write_table(table, buffer)
            
            # 上传到S3
            self.s3_client.put_object(
                Bucket=self.target_bucket,
                Key=target_key,
                Body=buffer.getvalue()
            )

    def _get_schema_from_df(self, df: pd.DataFrame) -> Dict:
        """从DataFrame获取Schema信息用于Glue表定义"""
        # 将列名中的斜杠替换为下划线
        df.columns = [col.replace('/', '_') for col in df.columns]
        
        # 定义特殊列的数据类型
        special_columns = {
            'cost_category': 'map<string,string>',
            'discount': 'map<string,double>',
            'product': 'map<string,string>',
            'resource_tags': 'map<string,string>'
        }
        
        # 基本数据类型映射
        type_mapping = {
            'object': 'string',
            'int64': 'bigint',
            'float64': 'double',
            'datetime64[ns]': 'timestamp',
            'bool': 'boolean'
        }
        
        columns = []
        for col_name, dtype in df.dtypes.items():
            # 检查是否是特殊列
            if col_name in special_columns:
                col_type = special_columns[col_name]
            else:
                col_type = type_mapping.get(str(dtype), 'string')
            
            columns.append({
                'Name': col_name,
                'Type': col_type
            })
        
        return columns

    def _ensure_database_exists(self):
        """确保 Athena 数据库存在，如果不存在则创建"""
        try:
            self.glue_client.get_database(Name=self.database_name)
            logger.info(f"Database {self.database_name} already exists")
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.info(f"Creating database {self.database_name}")
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': 'Database for Cost and Usage Reports analysis'
                }
            )

    def _delete_table_if_exists(self, table_name: str):
        """如果表存在，删除它"""
        try:
            self.glue_client.delete_table(
                DatabaseName=self.database_name,
                Name=table_name
            )
            logger.info(f"Deleted existing table {table_name}")
        except self.glue_client.exceptions.EntityNotFoundException:
            pass

    def _create_athena_table(self, granularity: str, columns: List[Dict]):
        """创建或更新Athena表"""
        # 确保数据库存在
        self._ensure_database_exists()
        
        # 删除现有的表
        table_name = f'cur_{granularity}'
        self._delete_table_if_exists(table_name)
        
        table_name = f'cur_{granularity}'
        
        # 分区列
        partition_cols = [
            {'Name': 'source_account_id', 'Type': 'string'},
            {'Name': 'report_name', 'Type': 'string'},
            {'Name': 'data', 'Type': 'string'},
            {'Name': 'billing_period', 'Type': 'string'}
        ]
        
        try:
            self.glue_client.create_table(
                DatabaseName=self.database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': columns,
                        'Location': f's3://{self.target_bucket}/{granularity}',
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'PartitionKeys': partition_cols,
                    'TableType': 'EXTERNAL_TABLE'
                }
            )
            logger.info(f"Created table {table_name}")
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.info(f"Table {table_name} already exists")

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
            cursor.execute("SELECT * FROM payer")
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
                dir_path = account[f'{granularity}_dir']
                if not dir_path:
                    continue

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
                            
                            # 计算文件名和修改时间的MD5
                            import hashlib
                            source_file = Path(source_key).name
                            last_modified = obj['LastModified'].isoformat()
                            content_to_hash = f"{source_file}{last_modified}"
                            file_hash = hashlib.md5(content_to_hash.encode()).hexdigest()[-16:]
                            
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
                            
                            # 直接上传最终文件
                            self._upload_parquet_to_s3(df, target_key)
                            
                            logger.info(f"Converted {source_key} to {target_key}")
                            
                            # 根据关键字段去重
                            if 'identity/LineItemId' in df.columns and 'identity/TimeInterval' in df.columns:
                                logger.info(f"原始数据行数: {len(df)}")
                                df = df.drop_duplicates(
                                    subset=[
                                        'identity/LineItemId',
                                        'identity/TimeInterval',
                                        'bill/PayerAccountId'
                                    ],
                                    keep='last'  # 保留最后一次出现的记录
                                )
                                logger.info(f"去重后数据行数: {len(df)}")
                            
                            # 更新 Athena 分区
                            try:
                                table_name = f'cur_{granularity}'
                                query = f"MSCK REPAIR TABLE {self.database_name}.{table_name}"
                                response = self.athena_client.start_query_execution(
                                    QueryString=query,
                                    ResultConfiguration={
                                        'OutputLocation': f's3://{self.target_bucket}/athena-results'
                                    }
                                )
                                logger.info(f"Started partition update for table {table_name}")
                            except Exception as e:
                                logger.error(f"Error updating partitions: {str(e)}")
                        except Exception as e:
                            logger.error(f"Error processing {source_key}: {str(e)}")
                            continue

                        
                        # 如果是第一个文件，创建或更新表结构
                        if not hasattr(self, f'{granularity}_table_created'):
                            columns = self._get_schema_from_df(df)
                            self._create_athena_table(granularity, columns)
                            setattr(self, f'{granularity}_table_created', True)
                        
                        # 上传parquet文件
                        self._upload_parquet_to_s3(df, target_key)
                        logger.info(f"Converted {source_key} to {target_key}")
                        
                        # 更新 Athena 分区
                        try:
                            table_name = f'cur_{granularity}'
                            query = f"MSCK REPAIR TABLE {self.database_name}.{table_name}"
                            response = self.athena_client.start_query_execution(
                                QueryString=query,
                                ResultConfiguration={
                                    'OutputLocation': f's3://{self.target_bucket}/athena-results'
                                }
                            )
                            logger.info(f"Started partition update for table {table_name}")
                            if 'identity/LineItemId' in df.columns and 'identity/TimeInterval' in df.columns:
                                logger.info(f"原始数据行数: {len(df)}")
                                df = df.drop_duplicates(
                                    subset=[
                                        'identity/LineItemId',
                                        'identity/TimeInterval',
                                        'bill/PayerAccountId'
                                    ],
                                    keep='last'  # 保留最后一次出现的记录
                                )
                                logger.info(f"去重后数据行数: {len(df)}")
                            
                            # 如果是第一个文件，创建或更新表结构
                            if not hasattr(self, f'{granularity}_table_created'):
                                columns = self._get_schema_from_df(df)
                                self._create_athena_table(granularity, columns)
                                setattr(self, f'{granularity}_table_created', True)
                            
                            # 上传parquet文件
                            self._upload_parquet_to_s3(df, target_key)
                            logger.info(f"Converted {source_key} to {target_key}")
                            
                            # 更新 Athena 分区
                            try:
                                table_name = f'cur_{granularity}'
                                query = f"MSCK REPAIR TABLE {self.database_name}.{table_name}"
                                response = self.athena_client.start_query_execution(
                                    QueryString=query,
                                    ResultConfiguration={
                                        'OutputLocation': f's3://{self.target_bucket}/athena-results'
                                    }
                                )
                                logger.info(f"Started partition update for table {table_name}")
                            except Exception as e:
                                logger.error(f"Error updating partitions: {str(e)}")
                            
                        except Exception as e:
                            logger.error(f"Error processing {source_key}: {str(e)}")
                            continue

def main():
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='Convert AWS CUR files to Parquet format')
    parser.add_argument('--full', action='store_true', 
                      help='执行全量同步，与 --hour 参数互斥')
    parser.add_argument('--hour', type=int,
                      help='同步指定小时数内有变动的文件，默认24小时')
    parser.add_argument('--payer', type=str,
                      help='指定要同步的 AWS Payer Account ID')
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
