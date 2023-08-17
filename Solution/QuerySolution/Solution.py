from main import AnalyticsApp
import argparse
from pyspark.sql import SparkSession
from pyspark import SparkConf


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customer.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/campaign.csv")
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/campaign_results.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/campaign_response.csv")
    return vars(parser.parse_args())


def main():
    spark_conf = SparkConf()
    spark_conf.set("spark.app.name", "DataFrameRunTimeErrorExample")
    spark_conf.set("spark.master", "local[2]")
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    params = get_params()
    shop_obj = ShoppingPatternApp(params)
    shop_obj.run(spark)
    

if __name__ == "__main__":
    main()
