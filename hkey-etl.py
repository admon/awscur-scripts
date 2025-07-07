import sys
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_date, year, month, concat_ws, format_string, col, lit

# 1. 解析 Glue Job 参数，BILLING_PERIOD 可选
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'TempDir'
] + (['JOB_BOOKMARK_OPTION'] if '--JOB_BOOKMARK_OPTION' in sys.argv else []) 
  + (['BILLING_PERIOD'] if '--BILLING_PERIOD' in sys.argv else []))

bookmark_option = args.get('JOB_BOOKMARK_OPTION', 'job-bookmark-disable') 

# 2. 获取账期参数或默认当前月份
billing_period = args.get('BILLING_PERIOD', datetime.utcnow().strftime('%Y-%m'))
print(f"[INFO] Using BILLING_PERIOD = {billing_period}")

# 3. 初始化 Glue 上下文
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 4. 从 Glue Catalog 读取 CUR 数据
cur_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="cid_data_export",
    table_name="cur2",
    transformation_ctx="cur_source",
    push_down_predicate="true"
)

# 5. 转换为 DataFrame 并构造 BILLING_PERIOD 列
df = cur_dyf.toDF()

df = df.withColumn("usage_date", to_date(col("line_item_usage_start_date"))) \
                      .withColumn("year", year(col("usage_date"))) \
                      .withColumn("month", month(col("usage_date"))) \
                      .withColumn("BILLING_PERIOD", concat_ws("-", col("year"), format_string("%02d", col("month"))))

# 6. 过滤无效数据
filtered_df = df.filter(
    (~col("line_item_line_item_type").isin("Discount", "Credit", "Refund")) &
    (~col("line_item_line_item_type").like("%Discount%")) &
    (col("BILLING_PERIOD") == billing_period)  # 仅保留当前账期
)

# 7. 可选：清理 S3 中该账期的输出分区目录
output_path = "s3://cur2-exports-2025/hkeyfiltered/"
bucket_name = "cur2-exports-2025"
prefix = f"hkeyfiltered/BILLING_PERIOD={billing_period}/"

s3 = boto3.resource("s3")
bucket = s3.Bucket(bucket_name)
objs_to_delete = list(bucket.objects.filter(Prefix=prefix))
if objs_to_delete:
    print(f"[INFO] Deleting {len(objs_to_delete)} objects under {prefix}")
    bucket.delete_objects(Delete={'Objects': [{'Key': obj.key} for obj in objs_to_delete]})
else:
    print(f"[INFO] No existing data to delete under {prefix}")

# 8. 写入 S3，按 BILLING_PERIOD 分区
filtered_df.write \
    .mode("append") \
    .partitionBy("BILLING_PERIOD") \
    .parquet(output_path)

# 9. 提交 Job
job.commit()

