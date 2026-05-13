import mysql.connector
import resources.dev.config as config
from src.main.utility.encrypt_decrypt import *

def get_mysql_connection():
    connection = mysql.connector.connect(
        host=decrypt(config.mysql_host),
        user=decrypt(config.mysql_user),
        password=decrypt(config.mysql_password),
        database=decrypt(config.mysql_database)
    )
    return connection


