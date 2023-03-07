import oakvar
import duckdb
from oakvar import BaseConverter



class Converter(BaseConverter):    
    def check_format(self,file):
        return file.name.endswith('.parquet') or file.name.endswith('.parquet.gz')
    
    def convert_file(self,file,start):

        conn = duckdb.connect()

        QUERY = f'''SELECT * FROM {file} LIMIT 1000 OFFSET {start-1}'''
        count = conn.execute(QUERY).df().to_dict('index')
        return count
