import luigi
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import os
import warnings
import sqlalchemy
from dotenv import load_dotenv
load_dotenv()
warnings.filterwarnings('ignore')


class Transform(luigi.Task):
    def run(self):
        try:
            # Establish connections to DWH
            _, dwh_engine = db_connection()
            
            #------------------------------------------------PART OF TRANSFORM DIMENSIONS TABLE-----------------------------------------------
            # Define DIR
            DIR_TRANSFORM_QUERY = os.getenv("DIR_TRANSFORM_QUERY")

            # Read transform query to final schema
            dim_customer_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/dim_customer.sql'
            )

            dim_product_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/dim_product.sql'
            )
            
            dim_seller_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/dim_seller.sql'
            )
            

            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()
            
            # Transform to final.dim_customer
            query = sqlalchemy.text(dim_customer_query)
            session.execute(query)
            
            # Transform to final.dim_product
            query = sqlalchemy.text(dim_product_query)
            session.execute(query)

            query = sqlalchemy.text(dim_seller_query)
            session.execute(query)
            
            # Commit transaction
            session.commit()
            
            # Close session
            session.close()        
            
            #------------------------------------------------PART OF FACT TABLE-----------------------------------------------
            
            # Read query to insert into fact tables
            fct_order_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/fct_order.sql'
            )

            fct_order_items_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/fct_order_items.sql'
            )        
            
            fct_order_payments_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/fct_order_payments.sql'
            ) 

            fct_order_reviews_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/fct_order_reviews.sql'
            ) 

            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()
            
            # Transform to final.fct_order
            query = sqlalchemy.text(fct_order_query)
            session.execute(query)

            query = sqlalchemy.text(fct_order_items_query)
            session.execute(query)        

            query = sqlalchemy.text(fct_order_payments_query)
            session.execute(query)

            query = sqlalchemy.text(fct_order_reviews_query)
            session.execute(query)

            # Commit transaction
            session.commit()
            
            # Close session
            session.close()  
            
        except Exception as e:
            print(f"Error during data transformation: {e}")

    def on_success(self):
        # Create marker file upon successful execution
        with self.output().open('w') as f:
            f.write('Transformation completed successfully.')


if __name__ == "__main__":
    luigi.run()