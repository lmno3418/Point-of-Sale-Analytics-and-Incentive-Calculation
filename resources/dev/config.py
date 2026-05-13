import os
from dotenv import load_dotenv
	
# Load variables from .env
load_dotenv()

key = os.getenv("key")           #.env
iv = os.getenv("iv")             #.env
salt = os.getenv("salt")         #.env

#AWS Access And Secret key
aws_access_key = "CuHaGqVXapevoAQwFdHrefFgtTB1MT4BB6l+zoBGHEY="                            #encrypted 
aws_secret_key = "LvwdrS7YtoHUkR5alCH8W5IW7HveA9NsH0wHED1Q54QSTm1bqRKPpW7lwiHSB511"        #encrypted 
bucket_name = "7sgA1nI3qetBOfoTQm/C2vp8Gme2+3iN01GvFSoq2I0="                               #encrypted 

#S3 directory details
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
s3_sales_partitioned_data = "sales_partitioned_data_mart/"


#Database credential
# MySQL database connection properties
url = f"jdbc:mysql://localhost:3306/{os.getenv('mysql_database')}"      
properties = {
    "user": os.getenv("mysql_user"),                     #encrypted         
    "password": os.getenv("mysql_password"),                 #encrypted                    
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


local_directory = "/Users/lmno3418/Documents/PROJECTS/sem2-project/file_from_s3"
customer_data_mart_local_file = "/Users/lmno3418/Documents/PROJECTS/sem2-project/customer_data_mart"
sales_team_data_mart_local_file = "/Users/lmno3418/Documents/PROJECTS/sem2-project/sales_team_data_mart"
sales_team_data_mart_partitioned_local_file = "/Users/lmno3418/Documents/PROJECTS/sem2-project/sales_partition_data"
error_folder_path_local = "/Users/lmno3418/Documents/PROJECTS/sem2-project/error_files"


#mysql connection details
mysql_host = "8SJ7HB/kjcvHCWlKxutqzw=="          #encrypted
mysql_user = "VzlOvWeBgZ1QbDI+L1X9ig=="          #encrypted
mysql_password = "glpJpZveO9dmNG1jkwK3EQ=="      #encrypted
mysql_database = "Apz7Geks5zrxPhlS1OsrLQ=="      #encrypted