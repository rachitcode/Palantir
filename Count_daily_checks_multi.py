#       Script to perform daily checks on ISP Datasets on Delta Files
#
#       ver   date     author           comment
#       ===   ======== ================ =========================================================
#       1.0 # 20/02/20 Rachit Saxena   Initial version
from transforms.api import transform_df, incremental, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

INPUT_DIR = "Dir/Layer_3.1_Datasets"
OUTPUT_DIR = "Dir/Layer_3.1_Datasets"


def generate_transform(system_source_tuples):

    transforms = []

    for system_source_tuple in system_source_tuples:
        system, source = system_source_tuple

        @incremental(snapshot_inputs=['input_df'])
        @transform_df(
            Output("{path}/l31_{system}_{source}_test".format(path=OUTPUT_DIR, system=system, source=source)),
            input_df=Input("{path}/l31_{system}_{source}_all".format(path=INPUT_DIR, system=system, source=source)),
        )
        def checks_consolidated(input_df):
            return input_df \
                .groupBy(input_df.SOURCE_SYSTEM) \
                .agg(F.count(input_df.REF_INV_ID).alias('Count'), F.sum(input_df.DLVD_QTY_WT).alias('DLVD_QTY_WT'), F.sum(input_df.DLVD_QTY_VOL).alias('DLVD_QTY_VOL'), F.sum(input_df.INVG_VAL).alias('INVG_VAL')) \
                .withColumn("Timestamp", F.current_timestamp()) \
                .withColumn('Dataset_Name', lit('l31_isp_iop_line_all'))
        transforms.append(checks_consolidated)

    return transforms

# All the sources (_system> is the filename) - add more if necessary


sources = [
        "iop_line"
]


# System Names
systems = [
    "isp"
]
TRANSFORM_ARGS = []

for system in systems:
    for source in sources:
        TRANSFORM_ARGS.append((system, source))

TRANSFORMS = generate_transform(TRANSFORM_ARGS)
