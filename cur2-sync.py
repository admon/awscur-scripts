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
import argparse

# 配置日志
def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"cur_sync_{datetime.now().strftime('%m%d-%H%M')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()

class FileDownloader:
    def __init__(self, local_download_dir):
        self.local_download_dir = local_download_dir
        # 创建使用IAM角色的S3客户端用于上传
        self.backup_s3_client = boto3.client('s3')

    def download_files(self, account, full_sync, hours=24, tier=None, path_filter=None):
        try:
            # 创建账号特定的S3客户端用于下载
            source_s3_client = boto3.client(
                's3',
                aws_access_key_id=account['access_key_id'],
                aws_secret_access_key=account['secret_access_key'],
                region_name=account['region_name'] or 'us-east-1'
            )

            # 计算时间阈值
            time_threshold = None if full_sync else datetime.now(timezone.utc) - timedelta(hours=hours)

            # 根据 tier 参数决定要处理的目录
            dir_mapping = {
                'monthly': account['monthly_dir'],
                'daily': account['daily_dir'],
                'hourly': account['hourly_dir']
            }

            # 获取要处理的目录列表
            base_prefixes = []
            if tier:
                if dir_mapping[tier]:
                    base_prefixes.append(dir_mapping[tier])
            else:
                for t in ['monthly', 'daily', 'hourly']:
                    if dir_mapping[t]:
                        base_prefixes.append(dir_mapping[t])

            # 构建 data 和 metadata 目录的前缀列表
            prefixes = []
            for base_prefix in base_prefixes:
                # 添加 data 目录
                prefixes.append(base_prefix)
                # 添加对应的 metadata 目录
                metadata_prefix = base_prefix.replace('/data', '/metadata')
                if metadata_prefix != base_prefix:  # 只有当实际发生替换时才添加
                    prefixes.append(metadata_prefix)
                    logger.info(f"Adding metadata directory: {metadata_prefix}")

            paginator = source_s3_client.get_paginator('list_objects_v2')
            for prefix in prefixes:
                if not prefix:
                    continue
                for page in paginator.paginate(Bucket=account['bucket'], Prefix=prefix):
                    for obj in page.get('Contents', []):
                        file_key = obj['Key']
                        last_modified = obj['LastModified']

                        # 只处理过去24小时内修改过的文件（如果不是全量同步）
                        if time_threshold and last_modified < time_threshold:
                            continue

                        # 如果指定了分区路径，检查文件是否在该分区中
                        if path_filter and path_filter not in file_key:
                            continue

                        # 使用数据库中的路径判断文件类型和路径
                        current_dir = prefix  # 当前正在处理的目录
                        
                        # 确定文件类型和报告粒度
                        if current_dir.endswith('/data') or '/data/' in current_dir:
                            content_type = 'data'
                        elif current_dir.endswith('/metadata') or '/metadata/' in current_dir:
                            content_type = 'metadata'
                        else:
                            # 如果路径中没有明确的 data 或 metadata 标记，默认为 data
                            content_type = 'data'
                            
                        # 使用数据库中的路径判断报告粒度
                        if tier:
                            report_type = tier
                        else:
                            # 根据当前目录判断报告粒度
                            if current_dir == account['hourly_dir'] or current_dir.replace('/data', '/metadata') == account['hourly_dir'].replace('/data', '/metadata'):
                                report_type = 'hourly'
                            elif current_dir == account['daily_dir'] or current_dir.replace('/data', '/metadata') == account['daily_dir'].replace('/data', '/metadata'):
                                report_type = 'daily'
                            elif current_dir == account['monthly_dir'] or current_dir.replace('/data', '/metadata') == account['monthly_dir'].replace('/data', '/metadata'):
                                report_type = 'monthly'
                            else:
                                logger.warning(f"Cannot determine report type for directory: {current_dir}")
                                continue
                                
                        # 提取剩余路径和文件名
                        remaining_path = file_key[len(current_dir):].lstrip('/')
                        file_name = file_key.split('/')[-1]

                        # 构造本地下载路径
                        local_download_path = self.local_download_dir / f"{account['aws_id']}_{file_name}"
                        logger.info(f"下载文件: {file_key} -> {local_download_path}")

                        # 下载文件
                        source_s3_client.download_file(account['bucket'], file_key, str(local_download_path))

                        # 构造S3上传路径
                        s3_upload_path = f"{report_type}/{account['aws_id']}/{content_type}/{remaining_path}"
                        logger.info(f"备份文件: {local_download_path} -> {s3_upload_path}")

                        # 使用IAM角色上传文件
                        self.backup_s3_client.upload_file(
                            str(local_download_path),
                            os.getenv('BACKUP_BUCKET'),
                            s3_upload_path
                        )
        except Exception as e:
            logger.error(f"下载文件失败: {str(e)}")

def get_payer_accounts(db_config):
    """从数据库获取所有AWS账号信息"""
    try:
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

def main():
    load_dotenv()
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME')
    }

    parser = argparse.ArgumentParser(description='Sync CUR files from S3.')
    parser.add_argument('--full', action='store_true',
                      help='执行全量同步，与 --hour 参数互斥')
    parser.add_argument('--hour', type=int,
                      help='同步指定小时数内有变动的文件，默认24小时')
    parser.add_argument('--payer', type=str,
                      help='指定要同步的 AWS Payer Account ID')
    parser.add_argument('--tier', type=str, choices=['hourly', 'daily', 'monthly'],
                      help='指定要同步的数据粒度，不指定时处理所有粒度')
    parser.add_argument('--path', type=str,
                      help='指定要同步的分区路径，例如：BILLING_PERIOD=2025-01')
    args = parser.parse_args()

    # 检查参数冲突
    if args.full and args.hour is not None:
        parser.error("--full 和 --hour 参数不能同时使用")

    # 检查分区路径格式
    if args.path and not args.path.startswith('BILLING_PERIOD='):
        parser.error("--path 参数必须以 'BILLING_PERIOD=' 开头")

    local_download_dir = Path('downloads')
    local_download_dir.mkdir(exist_ok=True)
    downloader = FileDownloader(local_download_dir)

    # 设置同步小时数
    hours = None if args.full else (args.hour or 24)

    # 获取AWS账号信息
    accounts = get_payer_accounts(db_config)

    # 如果指定了payer，只处理指定的账号
    if args.payer:
        accounts = [acc for acc in accounts if acc['aws_id'] == args.payer]
        if not accounts:
            logger.error(f"找不到 Payer Account ID: {args.payer}")
            return

    for account in accounts:
        logger.info(f"处理账号: {account['name']} ({account['aws_id']})")
        downloader.download_files(account, args.full, hours, args.tier, args.path)

if __name__ == "__main__":
    main()