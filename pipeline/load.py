from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

def load():
    try:
        engines = db_connection()
        if engines is None:
            _, dwh_engine = engines

        # Confirm env vars
        DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
        DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
        
        #-------------------------------Truncate tables in public schema ------------------------
        # Read query to truncate public schema in dwh
        truncate_query = read_sql_file(
            file_path = f'{DIR_LOAD_QUERY}/public-truncate_tables.sql'
        )  
        
        # Split the SQL queries if multiple queries are present
        truncate_query = truncate_query.split(';')

        # Remove newline characters and leading/trailing whitespaces
        truncate_query = [query.strip() for query in truncate_query if query.strip()]
        
        # Create session
        Session = sessionmaker(bind = dwh_engine)
        session = Session()

        # Execute each query
        for query in truncate_query:
            query = sqlalchemy.text(query)
            session.execute(query)
        
        # Commit the transaction
        session.commit()
        
        # Close session
        session.close()      

        #-------------------------------Part Of Load to public schema ------------------------
        # Data to be loaded into public schema
        order_items = pd.read_csv(f'{DIR_TEMP_DATA}/public.order_items.csv')
        products = pd.read_csv(f'{DIR_TEMP_DATA}/public.products.csv')
        sellers = pd.read_csv(f'{DIR_TEMP_DATA}/public.sellers.csv')
        orders = pd.read_csv(f'{DIR_TEMP_DATA}/public.orders.csv')
        customers = pd.read_csv(f'{DIR_TEMP_DATA}/public.customers.csv')
        geolocation = pd.read_csv(f'{DIR_TEMP_DATA}/public.geolocation.csv')
        order_payments = pd.read_csv(f'{DIR_TEMP_DATA}/public.order_payments.csv')
        order_reviews = pd.read_csv(f'{DIR_TEMP_DATA}/public.order_reviews.csv')
        product_category_name_translation = pd.read_csv(f'{DIR_TEMP_DATA}/public.product_category_name_translation.csv')
        
        # Load to public schema
        # Load product_category_name_translation table
        product_category_name_translation.to_sql('product_category_name_translation', 
                                               con=dwh_engine, 
                                               if_exists='append', 
                                               index=False, 
                                               schema='public')


        # Load geolocation table
        geolocation.to_sql('geolocation', 
                         con=dwh_engine, 
                         if_exists='append', 
                         index=False, 
                         schema='public')
        
        # Load customers table
        customers.to_sql('customers', 
                       con=dwh_engine, 
                       if_exists='append', 
                       index=False, 
                       schema='public')
        
        # Load sellers table
        sellers.to_sql('sellers', 
                     con=dwh_engine, 
                     if_exists='append', 
                     index=False, 
                     schema='public')

        # Load products table
        products.to_sql('products', 
                       con=dwh_engine, 
                       if_exists='append', 
                       index=False, 
                       schema='public')

        # Load orders table
        orders.to_sql('orders', 
                    con=dwh_engine, 
                    if_exists='append', 
                    index=False, 
                    schema='public')
    
        # Load order_payments table
        order_payments.to_sql('order_payments', 
                            con=dwh_engine, 
                            if_exists='append', 
                            index=False, 
                            schema='public')
          
        # Load order_reviews table
        order_reviews.to_sql('order_reviews', 
                           con=dwh_engine, 
                           if_exists='append', 
                           index=False, 
                           schema='public')

        # Load order_items table    
        order_items.to_sql('order_items', 
                          con=dwh_engine, 
                          if_exists='append', 
                          index=False, 
                          schema='public')

            
        #-------------------------------Part Of Load to staging schema ------------------------
        # Read load query to staging schema
        order_items_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-order_items.sql'
        )
        
        products_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-products.sql'
        )
        
        sellers_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-sellers.sql'
        )
        
        orders_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-orders.sql'
        )
        
        customers_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-customers.sql'
        )
        
        geolocation_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-geolocation.sql'
        )
        
        order_payments_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-order_payments.sql'
        )
        
        order_reviews_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-order_reviews.sql'
        )
        
        product_category_name_translation_query = read_sql_file(
            file_path=f'{DIR_LOAD_QUERY}/stg-product_category_name_translation.sql'
        )
        
        ## Load Into Staging schema
        # List query
        load_stg_queries = [
            product_category_name_translation_query,
            geolocation_query,
            customers_query,
            orders_query,
            products_query,
            sellers_query,
            order_payments_query,
            order_items_query,
            order_reviews_query
        ]


        # Create session
        Session = sessionmaker(bind = dwh_engine)
        session = Session()

        # Execute each query
        for query in load_stg_queries:
            query = sqlalchemy.text(query)
            session.execute(query)
            
        session.commit()
        
        # Close session
        session.close()
        
        
    except Exception as e:
        print(f"Error loading data: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    load()