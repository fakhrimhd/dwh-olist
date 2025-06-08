from pipeline.extract import *
from pipeline.load import *
from pipeline.transform import *
from pipeline.utils.db_conn import *

# Execute the functions when the script is run
if __name__ == '__main__':
    extract()
    print("Extract completed.")
    load()
    print("Load completed.")
    transform()
    print("Transform completed.")