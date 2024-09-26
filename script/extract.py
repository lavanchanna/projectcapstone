# code for extraction
import pandas as pd
import json
from sqlalchemy import create_engine, text
from script.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, ORACLE_USER,ORACLE_PASSWORD, ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE
import logging

# Configure the logging
logging.basicConfig(
    filename='logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

# create mysql database commection
# mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/enterpriseretaildwh')
mysql_engine = create_engine(
    f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create Oracle engine
# oracle_engine = create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')

oracle_engine = create_engine(
    f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')


def load_csv_mysql(file_path, table_name):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, mysql_engine, if_exists='replace', index=False)


def load_xml_mysql(file_path, table_name):
    df = pd.read_xml(file_path, xpath='.//item')
    df.to_sql(table_name, mysql_engine, if_exists='replace', index=False)


def load_json_mysql(file_path, table_name):
    df = pd.read_json(file_path)
    df.to_sql(table_name, mysql_engine, if_exists='replace', index=False)


def load_oracle_to_mysql(query, table_name):
    df = pd.read_sql(query, oracle_engine)
    df.to_sql(table_name, mysql_engine, if_exists='replace', index=False)


if __name__ == "__main__":
    logger.info("Data Extraction started...... ")
    load_csv_mysql('data/sales_data.csv', 'staging_sales')
    load_csv_mysql('data/product_data.csv', 'staging_product')
    load_xml_mysql('data/inventory_data.xml', 'staging_inventory')
    load_json_mysql('data/supplier_data.json', 'staging_supplier')
    load_oracle_to_mysql("select * from stores", 'staging_store')
    logger.info("Data Extraction completed successfully...... ")