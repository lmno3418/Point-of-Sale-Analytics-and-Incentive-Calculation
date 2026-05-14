import shutil
import datetime
import os
from loguru import logger

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.read.aws_read import S3Reader
from src.main.utility.spark_session import spark_session

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import concat_ws, expr, lit, col
from src.main.write.parquet_writer import ParquetWriter


def main():
    spark = None
    connection = None
    cursor = None

    try:
        ################################### Get S3 client ############################################
        aws_access_key = config.aws_access_key
        aws_secret_key = config.aws_secret_key

        s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
        s3_client = s3_client_provider.get_client()

        response = s3_client.list_buckets()
        logger.info(f"List of Buckets: {response['Buckets']}")

        # Check if local dir already has files
        csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]

        connection = get_mysql_connection()
        cursor = connection.cursor()

        total_csv_files = []
        if csv_files:
            for file in csv_files:
                total_csv_files.append(file)

                # NOTE: this query is unsafe if file names are not quoted properly.
                statement = (
                    f"select distinct file_name from "
                    f"{decrypt(config.mysql_database)}.{config.product_staging_table} "
                    f"where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'"
                )

                logger.info(f"dynamically statement created: {statement}")
                cursor.execute(statement)
                data = cursor.fetchall()

                if data:
                    logger.info("Your last run was failed, please check")
                else:
                    logger.info("No record matched")
        else:
            logger.info("Last run successful!!!")

        cursor.close()
        connection.close()
        cursor = None
        connection = None

        try:
            s3_reader = S3Reader()
            folder_path = config.s3_source_directory
            s3_absolute_file_path = s3_reader.list_files(
                s3_client,
                decrypt(config.bucket_name),
                folder_path=folder_path,
            )

            logger.info(f"Absolute path on s3 bucket for csv file {s3_absolute_file_path}")
            if not s3_absolute_file_path:
                raise Exception(f"No files available at {folder_path}")

        except Exception as e:
            logger.error(f"Exited with error:- {e}")
            raise

        bucket_name = decrypt(config.bucket_name)
        local_directory = config.local_directory
        prefix = f"s3://{bucket_name}/"
        file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
        logger.info(f"File paths available in s3 bucket {bucket_name} are {file_paths}")

        downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
        downloader.download_files(file_paths)

        all_files = os.listdir(local_directory)
        logger.info(f"Files available in local directory {local_directory} are {all_files}")

        if all_files:
            csv_files = []
            error_files = []

            for file in all_files:
                full_path = os.path.join(local_directory, file)
                if file.endswith(".csv"):
                    csv_files.append(full_path)
                else:
                    error_files.append(full_path)

            if not csv_files:
                raise Exception("No csv data available to process the request")
        else:
            raise Exception("There is no data to process")

        logger.info("########### LISTING THE FILES THAT NEEDS TO BE PROCESSED ##############")
        logger.info(f"Listing the files that needs to be processed {csv_files}")

        logger.info("########### Creating Spark session ################")
        spark = spark_session()
        logger.info("########## Spark session created ########")

        logger.info("########## Checking the schema for data loaded in S3 ########")

        correct_files = []
        for data in csv_files:
            data_schema = (
                spark.read.format("csv")
                .option("header", "true")
                .load(data)
                .columns
            )

            logger.info(f"Schema of the file {data} is {data_schema}")
            logger.info(f"Mandatory columns are {config.mandatory_columns}")

            missing_columns = set(config.mandatory_columns) - set(data_schema)
            logger.info(f"Missing columns in the file {data} are {missing_columns}")

            if missing_columns:
                error_files.append(data)
            else:
                correct_files.append(data)

        logger.info(f"Correct files are {correct_files}")
        logger.info(f"Error files are {error_files}")

        logger.info("########## Moving Error data to error directory if any ##########")
        error_folder_local_path = config.error_folder_path_local

        if error_files:
            for file_path in error_files:
                if os.path.exists(file_path):
                    file_name = os.path.basename(file_path)
                    destination_path = os.path.join(error_folder_local_path, file_name)

                    shutil.move(file_path, destination_path)
                    logger.info(f"Moved error file {file_path} to {destination_path}")

                    source_prefix = config.s3_source_directory
                    destination_prefix = config.s3_error_directory

                    message = move_s3_to_s3(
                        s3_client,
                        decrypt(config.bucket_name),
                        file_path,
                        source_prefix,
                        destination_prefix,
                    )
                    logger.info(message)
                else:
                    logger.info(f"{file_path} does not exist")

        logger.info("########## Updating the staging table that we have started the process ##########")
        insert_statements = []
        current_time = datetime.datetime.now()
        formatted_date = current_time.strftime("%Y-%m-%d %H:%M:%S")

        if correct_files:
            for file in correct_files:
                file_name = os.path.basename(file)
                insert_statement = (
                    f"INSERT INTO {decrypt(config.mysql_database)}.{config.product_staging_table} "
                    f"(file_name, status, created_at) "
                    f"VALUES ('{file_name}', 'A', '{formatted_date}')"
                )
                insert_statements.append(insert_statement)

            logger.info(f"Insert statements for staging table are {insert_statements}")

            logger.info("*********** Connecting to mysql server ***************")
            connection = get_mysql_connection()
            cursor = connection.cursor()

            for statement in insert_statements:
                cursor.execute(statement)
                connection.commit()

            logger.info("Staging table updated successfully with status A")
            cursor.close()
            connection.close()
            cursor = None
            connection = None

        logger.info("********** Staging table updated Successfully ******************")
        logger.info("******** Fixing extra columns coming from source ************")

        schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("store_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("sales_date", DateType(), True),
            StructField("sales_person_id", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_cost", FloatType(), True),
            StructField("additional_column", StringType(), True)
        ])

        final_df_to_process = spark.createDataFrame([], schema=schema)

        for data in correct_files:
            data_df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(data)
            )

            data_schema = data_df.columns
            extra_columns = list(set(data_schema) - set(config.mandatory_columns))
            logger.info(f"Extra columns present at source is {extra_columns}")

            if extra_columns:
                data_df = (
                    data_df.withColumn("additional_column", concat_ws(",", *extra_columns))
                    .select(
                        "customer_id", "store_id", "product_name", "sales_date",
                        "sales_person_id", "price", "quantity", "total_cost",
                        col("additional_column")
                    )
                )
            else:
                data_df = (
                    data_df.withColumn("additional_column", lit(None))
                    .select(
                        "customer_id", "store_id", "product_name", "sales_date",
                        "sales_person_id", "price", "quantity", "total_cost",
                        col("additional_column")
                    )
                )

            final_df_to_process = final_df_to_process.unionByName(data_df, allowMissingColumns=True)

        logger.info("Final dataframe created with additional column if any")
        final_df_to_process.show(5, truncate=False)

        database_client = DatabaseReader(config.url, config.properties)

        customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
        product_table_df = database_client.create_dataframe(spark, config.product_table)
        product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)
        sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)
        store_table_df = database_client.create_dataframe(spark, config.store_table)

        s3_customer_store_sales_df_join = dimesions_table_join(
            final_df_to_process,
            customer_table_df,
            store_table_df,
            sales_team_table_df
        )

        logger.info("Final Enriched Data")
        s3_customer_store_sales_df_join.show(truncate=False)

        logger.info("write the data into Customer Data Mart")
        final_customer_data_mart_df = s3_customer_store_sales_df_join.select(
            "ct.customer_id",
            "ct.first_name", "ct.last_name", "ct.address",
            "ct.pincode",
            "ct.phone_number",
            "sales_date", "total_cost"
        )
        final_customer_data_mart_df.show(truncate=False)

        parquet_writer = ParquetWriter("overwrite", "parquet")
        parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

        s3_uploader = UploadToS3(s3_client)
        s3_uploader.upload_to_s3(
            config.s3_customer_datamart_directory,
            decrypt(config.bucket_name),
            config.customer_data_mart_local_file
        )

        logger.info("write the data into Sales Team Data Mart")
        final_sales_team_data_mart_df = s3_customer_store_sales_df_join.select(
            "store_id",
            "sales_person_id",
            "sales_person_first_name",
            "sales_person_last_name",
            "store_manager_name",
            "manager_id",
            "is_manager",
            "sales_person_address",
            "sales_person_pincode",
            "sales_date",
            "total_cost",
            expr("SUBSTRING(sales_date, 1, 7)").alias("sales_month")
        )

        final_sales_team_data_mart_df.show(truncate=False)
        parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)

        s3_uploader.upload_to_s3(
            config.s3_sales_datamart_directory,
            decrypt(config.bucket_name),
            config.sales_team_data_mart_local_file
        )

        final_sales_team_data_mart_df.write.format("parquet")\
            .option("header", "true")\
            .partitionBy("sales_month", "store_id")\
            .mode("overwrite")\
            .option("path", config.sales_team_data_mart_partitioned_local_file)\
            .save()

        s3_prefix = "sales_partitioned_data_mart"
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000

        for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_file_path = os.path.relpath(
                    local_file_path,
                    config.sales_team_data_mart_partitioned_local_file
                )
                s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
                s3_client.upload_file(local_file_path, decrypt(config.bucket_name), s3_key)

        logger.info("Calculating customer every month purchased amount")
        customer_mart_calculation_table_write(final_customer_data_mart_df)

        logger.info("Calculating sales team every month total sales and incentive")
        sales_mart_calculation_table_write(final_sales_team_data_mart_df)

        source_prefix = config.s3_source_directory
        destination_prefix = config.s3_processed_directory
        message = move_s3_to_s3(
            s3_client,
            decrypt(config.bucket_name),
            source_prefix,
            destination_prefix
        )
        logger.info(f"Moving source files from source to processed directory in s3 bucket: {message}")

        delete_local_file(config.local_directory)
        delete_local_file(config.customer_data_mart_local_file)
        delete_local_file(config.sales_team_data_mart_local_file)
        delete_local_file(config.sales_team_data_mart_partitioned_local_file)

        update_statements = []
        if correct_files:
            for file in correct_files:
                filename = os.path.basename(file)
                statements = f"""
                    UPDATE {decrypt(config.mysql_database)}.{config.product_staging_table}
                    SET status = 'I',
                        updated_date = '{formatted_date}'
                    WHERE file_name = '{filename}'
                """
                update_statements.append(statements)

            logger.info(f"Update statements for staging table are {update_statements}")

            connection = get_mysql_connection()
            cursor = connection.cursor()

            for statement in update_statements:
                cursor.execute(statement)
                connection.commit()

            logger.info("Staging table updated successfully with status I")

        # Keep Spark session alive for manual inspection of Spark UI
        if spark:
            logger.info("=" * 80)
            logger.info("Spark UI is available at: http://localhost:4040/jobs/")
            logger.info("=" * 80)
            user_input = input("Type 'close' to close the Spark session and exit: ").strip().lower()
            while user_input != "close":
                logger.warning(f"Invalid input: '{user_input}'. Please type 'close' to exit.")
                user_input = input("Type 'close' to close the Spark session and exit: ").strip().lower()
            logger.info("Closing Spark session as requested...")

    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass

        if connection:
            try:
                connection.close()
            except Exception:
                pass

        if spark:
            try:
                spark.stop()
                logger.info("Spark session closed")
            except Exception as e:
                logger.warning(f"Spark stop failed: {e}")


if __name__ == "__main__":
    main()