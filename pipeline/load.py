import luigi
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

class Load(luigi.Task):
    def run(self):
        try:
            engines = db_connection()
            if engines is None:
                _, dwh_engine = engines

            # Confirm env vars
            DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
            DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
            
            #-------------------------------Truncate tables in public schema ------------------------
            truncate_query = read_sql_file(
                file_path=f'{DIR_LOAD_QUERY}/public-truncate_tables.sql'
            )
            truncate_query = truncate_query.split(';')
            truncate_query = [query.strip() for query in truncate_query if query.strip()]
            
            Session = sessionmaker(bind=dwh_engine)
            session = Session()

            for query in truncate_query:
                query = sqlalchemy.text(query)
                session.execute(query)
            session.commit()
            session.close()

            #-------------------------------Part Of Load to public schema ------------------------
            tables = [
                ('product_category_name_translation', 'public.product_category_name_translation.csv'),
                ('geolocation', 'public.geolocation.csv'),
                ('customers', 'public.customers.csv'),
                ('sellers', 'public.sellers.csv'),
                ('products', 'public.products.csv'),
                ('orders', 'public.orders.csv'),
                ('order_payments', 'public.order_payments.csv'),
                ('order_reviews', 'public.order_reviews.csv'),
                ('order_items', 'public.order_items.csv'),
            ]

            for table_name, file_name in tables:
                data = pd.read_csv(f'{DIR_TEMP_DATA}/{file_name}')
                data.to_sql(table_name, con=dwh_engine, if_exists='append', index=False, schema='public')

            #-------------------------------Part Of Load to staging schema ------------------------
            staging_queries = [
                'stg-product_category_name_translation.sql',
                'stg-geolocation.sql',
                'stg-customers.sql',
                'stg-orders.sql',
                'stg-products.sql',
                'stg-sellers.sql',
                'stg-order_payments.sql',
                'stg-order_items.sql',
                'stg-order_reviews.sql',
            ]

            load_stg_queries = [read_sql_file(file_path=f'{DIR_LOAD_QUERY}/{query}') for query in staging_queries]

            session = Session()
            for query in load_stg_queries:
                query = sqlalchemy.text(query)
                session.execute(query)
            session.commit()
            session.close()

        except Exception as e:
            print(f"Error loading data: {e}")

    def output(self):
        return luigi.LocalTarget('load_data_to_dwh.txt')

# Execute the Luigi task
if __name__ == "__main__":
    luigi.run()
