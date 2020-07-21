#       Script to perform daily checks on ISP Datasets on Delta Files
#
#       ver   date     author           comment
#       ===   ======== ================ =========================================================
#       1.0 # 20/02/20 Rachit Saxena   Initial version
from transforms.api import transform_df, incremental, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.functions import col


@incremental(snapshot_inputs=['input_df'])
@transform_df(
    Output("/BP/Downstream-AirBP-DD-Insight_Hub/5 Testing/LCP Daily Checks/lcpcostelement"),
    input_df=Input("/BP/Downstream-AirBP-DD-Insight_Hub/3 Publish/Insight Hub/LCP/l6_all_lcpcostelement"),
)
def checks_consolidated(input_df):
    return input_df \
                .withColumn("TIMESTAMP", F.current_timestamp()) \
                .groupBy(input_df.source_system, (F.year(input_df.billing_date)).alias('YEAR'), (F.month(input_df.billing_date)).alias('MONTH'), input_df.country, input_df.plant, col("TIMESTAMP").alias("TIMESTAMP")) \
                .agg((F.count(input_df.source_system).alias('COUNT')), (F.sum(input_df.wf_cond_value_gc)).alias('GP_USD'))
