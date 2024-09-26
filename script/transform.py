import pandas as pd
import json
from sqlalchemy import create_engine, text
from script.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
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


def filter_sales_data():
    query = text("""SELECT * FROM staging_sales where sale_date>='2024-09-05'""")
    df = pd.read_sql(query, mysql_engine)
    df.to_sql('filtered_sales', mysql_engine, if_exists='replace', index=False)


def route_sales_data():
    query_high = """
    SELECT * FROM filtered_sales
    WHERE region = 'High';
    """
    df_high = pd.read_sql(query_high, mysql_engine)
    df_high.to_sql('high_sales', mysql_engine, if_exists='replace', index=False)

    query_low = """
    SELECT * FROM filtered_sales
    WHERE region = 'Low';
    """
    df_low = pd.read_sql(query_low, mysql_engine)
    df_low.to_sql('low_sales', mysql_engine, if_exists='replace', index=False)


def aggregate_sales_data():
    query = """
    SELECT 
        product_id,
        MONTH(sale_date) AS month,
        YEAR(sale_date) AS year,
        SUM(quantity * price) AS total_sales
    FROM filtered_sales
    GROUP BY product_id, MONTH(sale_date), YEAR(sale_date);
    """
    df = pd.read_sql(query, mysql_engine)
    df.to_sql('monthly_sales_summary_source', mysql_engine, if_exists='replace', index=False)


def join_sales_data():
    query = """
    SELECT 
        s.sales_id,
        s.product_id,
        p.product_name,
        s.store_id,
        st.store_name,
        s.quantity,
        (s.quantity * s.price) AS total_sales,
        s.sale_date
    FROM filtered_sales s
    JOIN staging_product p ON s.product_id = p.product_id
    JOIN staging_store st ON s.store_id = st.store_id;
    """
    df = pd.read_sql(query, mysql_engine)
    df.to_sql('sales_with_details', mysql_engine, if_exists='replace', index=False)


def aggregate_inventory_levels():
    query = """
    SELECT 
        store_id,
        SUM(quantity_on_hand) AS total_inventory
    FROM staging_inventory
    GROUP BY store_id;
    """
    df = pd.read_sql(query, mysql_engine)
    df.to_sql('aggregated_inventory_levels', mysql_engine, if_exists='replace', index=False)


if __name__ == "__main__":
    logger.info("Data Transfromation started...... ")
    filter_sales_data()
    route_sales_data()
    aggregate_sales_data()
    join_sales_data()
    aggregate_inventory_levels()
    logger.info("Data Transfromation completed successfully...... ")


