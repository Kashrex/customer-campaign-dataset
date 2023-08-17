from pyspark.sql import SparkSession

class AnalyticsApp:
    # Define schema for customer
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("has_credit_card", StringType(), True),
        StructField("has_checking_account", StringType(), True),
        StructField("has_debit_account", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("gender", StringType(), True)
        StructField("age", IntegerType(), True),
        StructField("education_level", StringType(), True),
        StructField("visits_last_3_months", IntegerType(), True)
        StructField("visits_last_12_months", IntegerType(), True)
    ])

    customer_path = '/tmp/input_data/customer.csv'
    customer_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'false').schema(customer_schema).load(customer_path)
    
    # Define schema for campaign result
    campaign_results_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("campaign_id", IntegerType(), True),
        StructField("num_products_before_campaign", IntegerType(), True),
        StructField("num_products_after_campaign", IntegerType(), True),
        StructField("new_credit_application_received", IntegerType(), True)
    ])

    campaign_results_path = '/tmp/input_data/campaign_results.csv'
    campaign_results_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'false').schema(campaign_results_schema).load(campaign_results_path)

        # Define schema for campaign response
    campaign_response_schema = StructType([
        StructField("campaign_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("response_received_in_24_hrs", IntegerType(), True),
        StructField("response_received_after_24_hrs", IntegerType(), True),
    ])

    campaign_response_path = '/tmp/input_data/campaign_response.csv'
    campaign_response_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'false').schema(campaign_response_schema).load(campaign_response_path)

    # Define schema for campaign info
    campaign_schema = StructType([
        StructField("campaign_id", IntegerType(), True),
        StructField("details", StringType(), True),
    ])

    campaign_path = '/tmp/input_data/campaign.csv'
    campaign_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'false').schema(campaign_schema).load(campaign_path)
    
        
        # Defining a new DataFrame containing customers who responded to any campaign
    responded_customers_df = campaign_response_df.filter(
        (campaign_response_df["response_received_in_24_hrs"] == 1) |
        (campaign_response_df["response_received_after_24_hrs"] == 1)
    )

    # Joining with the customer_df to get customer details
    top_customers_df = responded_customers_df.join(customer_df, on="customer_id", how="inner")


    # Selecting the desired columns
    output1_df = top_customers_df.select(
        "customer_id",
        "age",
        "city",
        "has_credit_card"
    )

    output1_df.show()

    # Filtering for female customers in New York City
    filtered_top_customers_df = top_customers_df.filter(
        (top_customers_df["gender"] == "female") &
        (top_customers_df["city"] == "New York City")
    )

    # Selecting top 50 customers based on their response count
    top_50_female_nyc_customers = filtered_top_customers_df \
        .groupBy("customer_id") \
        .agg({"response_received_in_24_hrs": "sum", "response_received_after_24_hrs": "sum"}) \
        .withColumnRenamed("sum(response_received_in_24_hrs)", "response_in_24_hrs") \
        .withColumnRenamed("sum(response_received_after_24_hrs)", "resonse_after_24_hrs") \
        .orderBy((col("response_in_24_hrs") + col("response_after_24_hrs")).desc()) \
        .limit(50)

    output2_df = customer_df.join(top_50_female_nyc_customers, on="campaign_id", how="left")

    output2_df.show()

    # Joining
    responded_education_df = responded_customers_df.join(customer_df, on="customer_id", how="inner")

    # Group by education level and count the number of customers per level
    customers_per_education_level = responded_education_df \
        .groupBy("education_level") \
        .count() \
        .orderBy("count", ascending=False)

    output3_df = customer_df.join(customers_per_education_level, on="campaign_id", how="left")

    output3_df.show()