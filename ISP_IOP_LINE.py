#       Script to calculate Latest Snapshot collectively on Delta Files
#
#       ver   date     author           comment
#       ===   ======== ================ =========================================================
#       1.0 # 26/08/19 Rachit Saxena   Initial version
from transforms.api import transform_df, Input, Output
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from pyspark.sql import functions as F

# Inside there is <Country>/<Source>_<Country>
INPUT_DIR = "/BP/Downstream-AirBP-SS-ISP/1 Raw/Production_Datasets/Layer_2_Datasets"
OUTPUT_DIR = "/BP/Downstream-AirBP-SS-ISP/1 Raw/Production_Datasets/Layer_3_Datasets"


def generate_transform(country, source):
    @transform_df(
        Output("{path}/{country}/l3_{source}_{country}".format(path=OUTPUT_DIR, country=country, source=source)),
        input_df=Input("{path}/{country}/l2_{source}_{country}".format(path=INPUT_DIR, country=country, source=source)),
    )
    def l2_to_l3(input_df):
        df = input_df
        df = df.sort(df.LAST_UPDT_DATE_TIME.desc())
        column_list = ["INV_ID", "ITEM_NUM", "ITEM_SEQ_NUM"]
        window = Window.partitionBy([col(x) for x in column_list]).orderBy(df['LAST_UPDT_DATE_TIME'].desc())
        df = df.select('*', row_number().over(window).alias('row_number')).filter(col('row_number') == 1)
        delete_is_null = F.isnull(F.col("LOG_DEL_IND"))
        df = df.where(delete_is_null)
        return df

    return l2_to_l3


# All the sources (_<Country> is the filename) - add more if necessary
sources = [
         "isp_iop_line_invoice_line"
]

# All the countries
countries = [
    "uk",
    "gr",
    "aa",
    "fr",
    "cy",
    "me",
    "mz",
    "tr",
    "za",
    "ch"
]

TRANSFORM_ARGS = []

for country in countries:
    for source in sources:
        TRANSFORM_ARGS.append((country, source))

TRANSFORMS = [generate_transform(*args) for args in TRANSFORM_ARGS]
