import pandas as pd
import json
from sqlalchemy import create_engine, text
from script.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
import logging

# Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

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


def load_sales_fact():
    query = text("""
    INSERT INTO fact_sales (sales_id, product_id, store_id, quantity, total_sales, sale_date)
    SELECT
        sales_id,
        product_id,
        store_id,
        quantity,
        total_sales,
        sale_date
    FROM sales_with_details;
    """)

    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_levels_by_store query.")
            conn.execute(query)
            conn.commit()
            logger.info("Data loaded into inventory_levels_by_store.")
    except Exception as e:
        logger.error("An error occurred while loading sales_fact: %s", e, exc_info=True)


def load_inventory_fact():
    query = text("""
    INSERT INTO fact_inventory (product_id, store_id, quantity_on_hand, last_updated)
    SELECT
        product_id,
        store_id,
        quantity_on_hand,
        last_updated
    FROM staging_inventory;
    """)

    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_levels_by_store query.")
            conn.execute(query)
            conn.commit()
            logger.info("Data loaded into inventory_levels_by_store.")
    except Exception as e:
        logger.error("An error occurred while loading fact_inventory: %s", e, exc_info=True)


def load_monthly_sales_summary():
    query = text("""
    INSERT IGNORE INTO monthly_sales_summary (product_id, month, year, total_sales)
    SELECT 
        product_id,
        month,
        year,
        total_sales
    FROM monthly_sales_summary_source;  -- Ensure this is the correct source table
    """)

    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_levels_by_store query.")
            conn.execute(query)
            conn.commit()
            logger.info("Data loaded into inventory_levels_by_store.")
    except Exception as e:
        logger.error("An error occurred while loading sales_fact: %s", e, exc_info=True)


def load_inventory_levels_by_store():
    query = text("""
    INSERT IGNORE INTO inventory_levels_by_store (store_id, total_inventory)
    SELECT 
        store_id,
        total_inventory
    FROM aggregated_inventory_levels;
    """)

    try:
        with mysql_engine.connect() as conn:
            logger.info("Executing load_inventory_levels_by_store query.")
            conn.execute(query)
            conn.commit()
            logger.info("Data loaded into inventory_levels_by_store.")
    except Exception as e:
        logger.error("An error occurred while loading inventory_levels_by_store: %s", e, exc_info=True)


if __name__ == "__main__":
    logger.info("Data Loading started...... ")
    load_sales_fact()
    load_inventory_fact()
    load_monthly_sales_summary()
    load_inventory_levels_by_store()
    logger.info("Data Loading completed successfully...... ")
