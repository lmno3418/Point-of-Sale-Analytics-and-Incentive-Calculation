import shutil
import datetime

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import * 
from src.main.utility.s3_client_object import *

import os
from loguru import logger
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import concat_ws, expr, lit

from src.main.write.parquet_writer import ParquetWriter



################################### Get S3 client ############################################
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key 

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# Now you can use s3_client for your S3 operations
response = s3_client.list_buckets()
# print (response)
logger.info(f"List of Buckets: {response['Buckets']}")

#check if local dir already has a file
#if file is there then check if same file is present in staging area with a status A
#if so then don't delete or re-run the file.
#else give error and do not procces the next files.

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
        
        statement = f"select distinct file_name from "\
                    f"{decrypt(config.mysql_database)}.{config.product_staging_table} "\
                    f"where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'"
                    
        logger.info(f"dynamically statement created: {statement}")
        cursor.execute(statement) 
        data = cursor.fetchall()
        if data:
            logger.info("Your lasst run was failed please check")
        else:
            logger.info("No record matched")
else:
    logger.info("Last run successfull!!!")
    
cursor.close()
connection.close()
        

try:
    s3_reader = S3Reader()
    
    # Bucket name should come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, decrypt(config.bucket_name), folder_path=folder_path)
    
    logger.info(f"Absolute path on s3 bucket for csv file {s3_absolute_file_path} ")
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No Data available to process ")
except Exception as e:
    logger.error(f"Exited with error:- {e}")
    raise e








bucket_name = decrypt(config.bucket_name)
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"

file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info(f"File paths available in s3 bucket {bucket_name} are {file_paths}")

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"File download error with message:- {e}")
    raise e

#get a list of all the files in local directory
all_files = os.listdir(local_directory)
logger.info(f"Files available in local directory {local_directory} are {all_files}")

#Filter files with .csv in their name and create absolute path
if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if file.endswith('.csv'):
            csv_files.append(os.path.join(local_directory, file))
        else:
            error_files.append(os.path.join(local_directory, file))
        
    if not csv_files:
        logger.info("No csv data available to process the request")
        raise Exception("No csv data available to process the request")

else:
    logger.error("there is no data to process")
    raise Exception("there is no data to process")

## ###################make a csv lines convert into a list of comma seperated ###############
## csv_files=str(csv_files)[1:-1]

logger.info("###########LISTING THE FILES THAT NEEDS TO BE PROCESSED ##############")
logger.info(f"Listing the files that needs to be processed {csv_files}")

logger.info("###########Creating Spark session################")

spark = spark_session()

logger.info("##########Spark session created########")



#check the required column in the schema of csv files
#if not required columns keep it in a list or error_files
#else union all the data into a single dataframe 

logger.info("##########Checking the schema for data loaded in S3########")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
                    .option("header", "true")\
                    .load(data).columns
    logger.info(f"Schema of the file {data} is {data_schema}")
    logger.info(f"Mandatory columns are {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns in the file {data} are {missing_columns}")
    
    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing file for the {data} ")
        correct_files.append(data)

logger.info(f"Correct files are {correct_files}")
logger.info(f"Error files are {error_files}")
logger.info("##########Moving Error data to error directory if any##########")

#move the error files to error directory in local

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
            
            message = move_s3_to_s3(s3_client, decrypt(config.bucket_name), file_path, source_prefix, destination_prefix)
            logger.info(message)
        else:
            logger.info(f"{file_path} does not exist")
else:
    logger.info("########There is No file available at our dataset##########")
        
        
#additiona columns needs to be taken care of
#determine extra columns


#before running the process
#stage table needs to be updated with status A (Active) or I (InActive) 
logger.info("##########Updating the staging table that we have started the process ##########")
insert_statements = []
dr_name=config.mysql_database
current_time = datetime.datetime.now()
formatted_date = current_time.strftime('%Y-%m-%d %H:%M:%S')

if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        insert_statement = f"INSERT INTO {decrypt(config.mysql_database)}.{config.product_staging_table} (file_name, status, created_at) VALUES ('{file_name}', 'A', '{formatted_date}')"
        insert_statements.append(insert_statement)
        
    logger.info(f"Insert statements for staging table are {insert_statements}")
    
    
    logger.info("***********Connecting to mysql server***************")
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        logger.info("****************Connected to mysql server successfully************")

        for statement in insert_statements:
            cursor.execute(statement)
            connection.commit()
        logger.info("Staging table updated successfully with status A")
        cursor.close()
        connection.close()
    except Exception as e:
        logger.info("********there is no file to process")
        logger.info("********No Data Available with correct files************")
        logger.error(f"Error while updating staging table with message:- {e}")
        raise e


logger.info("**********Stagging table updated Successfully ******************")



logger.info("********Fixing extra columns coming from source************")


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


#connecting with Database Reader
# database_client = DatabaseReader(config.url,config.properties)
# logger.info("************** creating empty dataframe *******************")
# final_df_to_process = database_client.create_dataframe(spark, "empty_df_create_table'")



final_df_to_process = spark.createDataFrame([], schema=schema)
# Create a new column with concatenated values of extra columns
for data in correct_files:
    data_df = spark.read.format("csv") \
        .option ("header", "true") \
        .option("inferSchema", "true") \
        .load (data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))

    logger.info(f"Extra columns present at source is {extra_columns}")

    if extra_columns:
        data_df = data_df.withColumn("Additional_column", concat_ws(",", *extra_columns))\
        .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "Additional_column")
        
        logger.info(f"processed {data} and added 'additional_column'")
    else:
        data_df = data_df.withColumn("Additional_column", lit(None))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "Additional_column")
            
            
    final_df_to_process = final_df_to_process.union(data_df)

# final_df_to_process = data_df

logger.info("**************Final dataframe created with additional column if any*******************")
final_df_to_process.show(5)



#Enrich the data from all dimension table
#also create a datamart for sales_team and their incentive, address and all
#another datamart for customer who bought how much each days of month
#for every month there should be a file and inside that
#there should be a store_id segrigation
#Read the data from parquet and generate a csv file
#In which there will be a sales_person_name, sales_person_store_id
#sales_person_total_billing_done_for_each_month, total_incentive


#Connecting with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)

#creating df for all tables #customer table
logger.info("************** Loading customer table into customer_table_df *****************")
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)

#product table
logger.info("************** Loading product table into product_table_df *******************")
product_table_df = database_client.create_dataframe(spark,config.product_table)

#product staging table
logger.info("************** Loading product staging table into product_staging_table_df *******************")
product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)

#sales team table
logger.info("************** Loading sales team table into sales_team_table_df *******************")
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)

#store table
logger.info("************** Loading store table into store_table_df *******************")
store_table_df = database_client.create_dataframe(spark,config.store_table)

#joining all the dimension tables with fact table
logger.info("************** Joining all dimension table (final_df_to_process) with fact table (customer_table_df) *******************")
s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)
#Final enriched data
logger.info("************ Final Enriched Data ********************")
s3_customer_store_sales_df_join.show() 





#Write the customer data into customer data mart in parquet format
#file will be written to local first
#wove the RAW data to s3 bucket for reporting tool
#Write reporting data into MySQL table also

logger.info("*************** write the data into Customer Data Mart **********")

final_customer_data_mart_df = s3_customer_store_sales_df_join\
    .select("ct.customer_id",
    "ct.first_name","ct.last_name", "ct.address" ,
    "ct.pincode",
    "ct.phone_number",
    "sales_date", "total_cost")

logger.info("*************** Final Data for customer Data Mart **********")

final_customer_data_mart_df.show()



parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

logger.info(f"*************** Customer Data Mart written in parquet format in local file system{config.customer_data_mart_local_file} **********")



#Move data to s3 bucket for customer_data_mart
logger.info("*************** Moving Customer Data Mart from local to s3 bucket **********")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, decrypt(config.bucket_name),config.customer_data_mart_local_file)
logger.info(f"*************** Upload Message: {message} **********")


#sales data mart
logger.info("*************** write the data into Sales Team Data Mart **********")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
    .select("store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name", 
    "store_manager_name", "manager_id", "is_manager", "sales_person_address", "sales_person_pincode",
    "sales_date", "total_cost", 
    expr("SUBSTRING(sales_date, 1, 7)").alias("sales_month"))
    
logger.info("*************** Final Data for Sales Team Data Mart **********")

final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)

logger.info(f"*************** Sales Team Data Mart written in parquet format in local file system{config.sales_team_data_mart_local_file} **********")  
    

#move data to s3 bucket for sales_team_data_mart
logger.info("*************** Moving Sales Team Data Mart from local to s3 bucket **********")
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, decrypt(config.bucket_name),config.sales_team_data_mart_local_file)
logger.info(f"*************** Upload Message: {message} **********")


#Also writting the data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
    .option("header", "true")\
    .partitionBy("sales_month", "store_id")\
    .mode("overwrite")\
    .option("path", config.sales_team_data_mart_partitioned_local_file)\
    .save()
    


#move data to s3 bucket for sales_team_data_mart partitioned data
logger.info("*************** Moving Sales Team Data Mart partitioned data from local to s3 bucket **********")
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000

for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, decrypt(config.bucket_name), s3_key)
        
        
#calculation for customer mart
#find out the customer total purchase every month
#write the data into MySQL table
logger.info("******Calculating customer every month purchased amount *******")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("******Calculation of customer mart done and written into the table*********")


#calculation for sales team mart
#find out the total sales done by each sales person every month 
#Give the top performer 1% incentive of total sales of the month
#Rest sales person will get nothing 
#write the data into MySQL table
        
logger.info("******Calculating sales team every month total sales and incentive *******")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("******Calculation of sales team mart done and written into the table*********")


######### Last Step #############
#move the file in S3 into processed folder and delete local files

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, decrypt(config.bucket_name), source_prefix, destination_prefix)
logger.info(f"*************** Moving source files from source to processed directory in s3 bucket with message: {message} **********")


logger.info("*************** Deleting sales data from local files **********")
delete_local_file(config.local_directory)
logger.info("*************** Local files deleted successfully **********")

logger.info("************ Deleting Customer Data Mart local file **********")
delete_local_file(config.customer_data_mart_local_file)
logger.info("************ Customer Data Mart local file deleted successfully **********")

logger.info("************ Deleting Sales Team Data Mart local file **********")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("************ Sales Team Data Mart local file deleted successfully **********")

logger.info("************ Deleting Sales Team Data Mart partitioned local file **********")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("************ Sales Team Data Mart partitioned local file deleted successfully **********")



#update the status of staging table
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
    
    
    logger.info("***********Connecting to mysql server for updating staging table status***************")
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        logger.info("****************Connected to mysql server successfully************")

        for statement in update_statements:
            cursor.execute(statement)
            connection.commit()
        logger.info("Staging table updated successfully with status I")
        cursor.close()
        connection.close()
        

        
        sys.exit()
        input("Press Enter to exit...")
        
        
    except Exception as e:
        logger.info("********there is no file to process")
        logger.info("********No Data Available with correct files************")
        logger.error(f"Error while updating staging table with message:- {e}")
        raise e





#closing spark session
spark.stop()
logger.info("#######Spark session closed##########")





    

