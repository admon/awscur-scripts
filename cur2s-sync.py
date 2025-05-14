import os
import boto3
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import gzip
import shutil
from dotenv import load_dotenv

# 配置日志
def setup_logger():
    # 创建logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # 创建日志目录
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # 创建文件处理器，使用当前日期作为日志文件名
    log_file = log_dir / f"cur_sync_{datetime.now().strftime('%m%d-%H%M')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

class CURSync:
    def __init__(self):
        load_dotenv()
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME')
        }
        self.local_download_dir = Path('downloads')
        self.local_download_dir.mkdir(exist_ok=True)
        
        # 初始化S3客户端
        self.s3_client = boto3.client('s3')
        
    def get_payer_accounts(self):
        """从数据库获取所有AWS账号信息"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM payer")
            accounts = cursor.fetchall()
            cursor.close()
            conn.close()
            return accounts
        except Exception as e:
            logger.error(f"获取账号信息失败: {str(e)}")
            raise

    def convert_to_parquet(self, gzip_file, remaining_path, dir_type, aws_id):
        """将gzip文件转换为parquet格式"""
        try:
            # 读取gzip文件
            with gzip.open(gzip_file, 'rt') as f:
                df = pd.read_csv(f, low_memory=False)
            
            # 生成parquet文件名，保持原始文件名结构
            parquet_file = gzip_file.with_suffix('.parquet')
            
            # 保存为parquet格式
            df.to_parquet(parquet_file, engine='pyarrow')
            logger.info(f"转换文件: {gzip_file} -> {parquet_file}")
            
            # 构建目标S3键，使用新的目录结构，完全替换文件扩展名
            s3_key = f"{dir_type}/{aws_id}/{remaining_path.rsplit('.', 2)[0]}.parquet"
            
            # 同步到目标S3
            self.s3_client.upload_file(
                str(parquet_file),
                os.getenv('TARGET_BUCKET'),
                s3_key
            )
            logger.info(f"同步文件到S3: {parquet_file} -> {s3_key}")
            
            return parquet_file
        except Exception as e:
            logger.error(f"转换文件失败: {str(e)}")
            raise

    def download_cur_files(self, account):
        """下载指定账号的CUR文件"""
        try:
            # 创建账号特定的S3客户端
            s3_client = boto3.client(
                's3',
                aws_access_key_id=account['access_key_id'],
                aws_secret_access_key=account['secret_access_key'],
                region_name=account['region_name'] or 'us-east-1'
            )
            
            # 计算三个月前的日期（使用UTC时区）
            three_months_ago = datetime.now(timezone.utc) - timedelta(days=90)
            
            # 获取所有CUR文件
            paginator = s3_client.get_paginator('list_objects_v2')
            for prefix in [account['monthly_dir'], account['daily_dir'], account['hourly_dir']]:
                if not prefix:
                    continue
                
                # 确定目录类型
                dir_type = 'monthly' if prefix == account['monthly_dir'] else \
                          'daily' if prefix == account['daily_dir'] else 'hourly'
                    
                for page in paginator.paginate(Bucket=account['bucket'], Prefix=prefix):
                    for obj in page.get('Contents', []):
                        file_key = obj['Key']
                        last_modified = obj['LastModified']
                        
                        # 只处理最近三个月的文件
                        if last_modified < three_months_ago:
                            continue
                            
                        # 下载文件
                        local_path = self.local_download_dir / f"{account['aws_id']}_{file_key.split('/')[-1]}"
                        s3_client.download_file(account['bucket'], file_key, str(local_path))
                        logger.info(f"下载文件: {file_key} -> {local_path}")
                        
                        # 备份到rdinfra账号的S3，移除DB字段相关的目录名
                        path_parts = file_key.split('/')
                        # 移除第一级目录（DB字段对应的目录）
                        remaining_path = '/'.join(path_parts[3:])
                        backup_key = f"{dir_type}/{account['aws_id']}/{remaining_path}"
                        self.s3_client.upload_file(
                            str(local_path),
                            os.getenv('BACKUP_BUCKET'),
                            backup_key
                        )
                        logger.info(f"备份文件: {local_path} -> {backup_key}")
                        
                        # 转换文件格式
                        try:
                            parquet_file = self.convert_to_parquet(
                                local_path, 
                                remaining_path, 
                                dir_type,
                                account['aws_id']
                            )
                            
                            # 清理本地文件
                            parquet_file.unlink()
                            local_path.unlink()
                            
                        except Exception as e:
                            logger.error(f"处理文件失败 {local_path}: {str(e)}")
                            continue
                        
        except Exception as e:
            logger.error(f"下载文件失败: {str(e)}")
            raise

    def cleanup_old_files(self):
        """清理超过三个月的本地文件"""
        try:
            three_months_ago = datetime.now(timezone.utc) - timedelta(days=90)
            for file in self.local_download_dir.glob('*.gz'):
                file_mtime = datetime.fromtimestamp(file.stat().st_mtime, timezone.utc)
                if file_mtime < three_months_ago:
                    file.unlink()
                    logger.info(f"清理旧文件: {file}")
        except Exception as e:
            logger.error(f"清理文件失败: {str(e)}")
            raise

    def run(self):
        """运行同步流程"""
        try:
            accounts = self.get_payer_accounts()
            for account in accounts:
                logger.info(f"处理账号: {account['name']} ({account['aws_id']})")
                
                # 下载并处理CUR文件
                self.download_cur_files(account)
            
            # 清理旧文件
            self.cleanup_old_files()
            
        except Exception as e:
            logger.error(f"同步过程失败: {str(e)}")
            raise

if __name__ == "__main__":
    sync = CURSync()
    sync.run() 