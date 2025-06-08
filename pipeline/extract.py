import pandas as pd
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
import warnings
import os
warnings.filterwarnings('ignore')

from dotenv import load_dotenv
load_dotenv()

def extract():
    
    # Define DIR
    DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
    DIR_EXTRACT_QUERY = os.getenv("DIR_EXTRACT_QUERY")
    
    try:
        
        # Define tables to be extracted from db sources
        tables_to_extract = ['public.order_items', 
                            'public.products', 
                            'public.sellers',
                            'public.orders',
                            'public.customers',
                            'public.geolocation',
                            'public.order_payments', 
                            'public.order_reviews', 
                            'public.product_category_name_translation']
        
        # Define db connection engine
        src_engine, _ = db_connection()
        
        # Define the query using the SQL content
        extract_query = read_sql_file(
            file_path = f'{DIR_EXTRACT_QUERY}/all-tables.sql'
        )
        
        
        for index, table_name in enumerate(tables_to_extract):

            # Read data into DataFrame
            df = pd.read_sql_query(extract_query.format(table_name = table_name), src_engine)

            # Write DataFrame to CSV
            df.to_csv(f"{DIR_TEMP_DATA}/{table_name}.csv", index=False)
                
    except Exception:
        raise Exception("Failed to extract data")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    extract()