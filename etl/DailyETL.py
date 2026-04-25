import pandas as pd
from etl.config import CONFIG
from pyspark.sql import SparkSession

class DailyETL:
    def __init__(self):
        self.config = CONFIG

    def extract(self, entity: str) -> pd.DataFrame:
        df = pd.read_csv(self.config["RAW_DATA_LOCATION"] + entity + '.csv')
        return df
    
    def transform(self, df: pd.DataFrame, entity: str) -> pd.DataFrame:
        if entity == 'customers':
            df['name'].fillna('Unknown', inplace=True)
            df['email'].fillna('Unknown', inplace=True)
            df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')

        elif entity == 'orders':
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    
        return df

    def load(self, df: pd.DataFrame, entity: str):
        spark = SparkSession.builder \
            .appName("IcebergExample") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
            .config("spark.sql.catalog.analytics", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.analytics.type", "hadoop") \
            .config("spark.sql.catalog.analytics.warehouse", "file:///app/warehouse") \
            .config("spark.sql.defaultCatalog", "analytics")\
            .getOrCreate()

        spark_df = spark.createDataFrame(df)
        spark_df.writeTo(f"dw_{entity}").createOrReplace()


    def execute(self):
        for entity in self.config["DATA_ENTITIES"]:
            df = self.extract(entity)
            df = self.transform(df, entity)
            self.load(df, entity)
            