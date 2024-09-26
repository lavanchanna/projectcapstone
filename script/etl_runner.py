import script.extract as etraction
import script.transform as transformation
import script.load as loading

if __name__ == "__main__":
    etraction.load_csv_mysql('data/sales_data.csv', 'staging_sales')
    etraction.load_csv_mysql('data/product_data.csv', 'staging_product')
    etraction.load_xml_mysql('data/inventory_data.xml', 'staging_inventory')
    etraction.load_json_mysql('data/supplier_data.json', 'staging_supplier')
    etraction.load_oracle_to_mysql("select * from stores", 'staging_store')
    etraction.logger.info("Data Extraction completed successfully...... ")

    transformation.filter_sales_data()
    transformation.route_sales_data()
    transformation.aggregate_sales_data()
    transformation.join_sales_data()
    transformation.aggregate_inventory_levels()

    loading.load_sales_fact()
    loading.load_inventory_fact()
    loading.load_monthly_sales_summary()
    loading.load_inventory_levels_by_store()
