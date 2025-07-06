import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, to_date, concat_ws, format_string

# 1. 初始化 Glue 上下文和作业
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir', 'JOB_BOOKMARK_OPTION'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. 从 Glue Data Catalog 读取源数据
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="cid_data_export",
    table_name="cur2",
    transformation_ctx="hkeysource0",
    push_down_predicate="true"
)

# 3. 将 DynamicFrame 转换为 Spark DataFrame
df = datasource0.toDF()

# 3.5. 添加 year, month 以及 BILLING_PERIOD 字段
df_with_date_cols = df.withColumn("usage_date", to_date(col("line_item_usage_start_date"))) \
                      .withColumn("year", year(col("usage_date"))) \
                      .withColumn("month", month(col("usage_date"))) \
                      .withColumn("BILLING_PERIOD", concat_ws("-", col("year"), format_string("%02d", col("month"))))

# 4. 应用过滤条件
filtered_df = df_with_date_cols.filter(
    (~col("line_item_line_item_type").isin('Discount', 'Credit', 'Refund')) &
    (~col("line_item_line_item_type").like('%Discount%'))
)

# 5. 转换回 DynamicFrame（可选）
filtered_data = DynamicFrame.fromDF(filtered_df, glueContext, "filtered_data")

# 6. 再转换为 Spark DataFrame 并写入 S3
filtered_df_to_write = filtered_data.toDF()

filtered_df_to_write.write.parquet(
    path="s3://cur2-exports-2025/hkeyfiltered/",
    mode="overwrite",
    partitionBy=["BILLING_PERIOD"]
)

# 7. 提交作业
job.commit()

