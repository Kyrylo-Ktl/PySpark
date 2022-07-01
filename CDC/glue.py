import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import when


def rename_columns(df: DataFrame, col_names: list) -> DataFrame:
    for old, new in zip(df.columns, col_names):
        df = df.withColumnRenamed(old, new)
    return df


def update(df: DataFrame, row: Row) -> DataFrame:
    for name in df.columns:
        if name == 'id':
            continue
        df.withColumn(
            name,
            when(df.id == row.id, row[name]).otherwise(df[name]),
        )
    return df


def insert(df: DataFrame, row: Row) -> DataFrame:
    new_row = spark.createDataFrame([row], df.columns)
    return df.union(new_row)


def delete(df: DataFrame, row: Row) -> DataFrame:
    return df.filter(df.id != row.id)


ACTIONS = {
    'U': update,
    'I': insert,
    'D': insert,
}

TARGET_BUCKET = 'cdc-pyspark-output'
args = getResolvedOptions(sys.argv, ['s3_target_path_key', 's3_target_path_bucket'])

SOURCE_BUCKET = args['s3_target_path_bucket']
print(f'BUCKET: {SOURCE_BUCKET}')

SOURCE_FILE = args['s3_target_path_key']
print(f'FILE: {SOURCE_FILE}')

spark = SparkSession.builder.appName('CDC').getOrCreate()
input_file_path = f's3a://{SOURCE_BUCKET}/{SOURCE_FILE}'
final_file_path = f's3a://{TARGET_BUCKET}/output'
tempFilePath = f's3a://{TARGET_BUCKET}/tmp'
COLUMNS = ['id', 'FullName', 'City']

if 'LOAD' in input_file_path:
    load_df = spark.read.csv(input_file_path)
    load_df = rename_columns(load_df, col_names=COLUMNS)
    load_df.write.mode('overwrite').csv(final_file_path)
else:
    update_df = spark.read.csv(input_file_path)
    actions = update_df.select('_c0').rdd.flatMap(lambda x: x)
    update_df = rename_columns(update_df.drop('_c0'), col_names=COLUMNS)

    final_df = spark.read.csv(final_file_path)
    final_df = rename_columns(final_df, col_names=COLUMNS)

    for action, values in zip(actions.collect(), update_df.collect()):
        print(f'OPERATION: {action}, ROW: {values}')
        final_df = ACTIONS[action](final_df, values)

    final_df.write.mode('overwrite').csv(tempFilePath)
    final_df = spark.read.csv(tempFilePath)
    final_df.write.mode('overwrite').csv(final_file_path)
