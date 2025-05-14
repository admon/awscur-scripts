# AWS CUR 数据同步工具

这个工具用于从多个AWS账号同步CUR（Cost and Usage Report）数据，并将其转换为Parquet格式存储在S3中。

## 功能特点

1. 从多个AWS账号下载CUR数据导出文件
2. 将gzip格式的文件转换为parquet格式
3. 同步parquet文件到目标S3存储桶
4. 自动备份原始gzip文件
5. 自动清理超过三个月的本地文件

## 安装要求

- Python 3.8+
- MySQL数据库
- AWS账号访问权限

## 安装步骤

1. 克隆代码库：
```bash
git clone <repository-url>
cd aws-cur-sync
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 配置环境变量：
```bash
cp .env.example .env
```
编辑.env文件，填入相应的配置信息。

## 配置说明

在.env文件中需要配置以下信息：

- 数据库连接信息（DB_HOST, DB_USER, DB_PASSWORD, DB_NAME）
- S3存储桶信息（BACKUP_BUCKET, TARGET_BUCKET）
- AWS凭证（AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY）

## 数据库表结构

程序使用MySQL数据库中的payer表来存储AWS账号信息，表结构如下：

```sql
CREATE TABLE payer (
    created_at DATETIME NOT NULL,
    update_at DATETIME NOT NULL,
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    aws_id VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    access_key_id VARCHAR(255) NOT NULL,
    secret_access_key VARCHAR(255) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    monthly_dir VARCHAR(255) NOT NULL,
    daily_dir VARCHAR(255) NOT NULL,
    hourly_dir VARCHAR(255) NOT NULL,
    region_name VARCHAR(255)
);
```

## 使用方法

1. 确保数据库中的payer表已经正确配置了所有AWS账号信息
2. 运行程序：
```bash
python aws_cur_sync.py
```

## 注意事项

1. 确保EC2实例有足够的磁盘空间用于临时存储文件
2. 确保EC2实例的IAM角色有权限访问目标S3存储桶
3. 建议使用crontab设置定时任务，定期运行同步程序

## 日志

程序运行日志会输出到控制台，包含以下信息：
- 文件下载状态
- 文件转换状态
- 文件同步状态
- 错误信息（如果有） # cur2parquet
