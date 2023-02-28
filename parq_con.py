from typing import Tuple
import duckdb
from oakvar import BaseConverter



class ParquetConverter(object):
    
    def __init__(self) -> None:
        self.format_name = 'parquet'
    
    def check_format(self,f):
        return f.name.endswith('.parquet') or f.name.endswith('.parquet.gz')
        
        
    def setup(self,f,chunk) ->Tuple[BaseConverter]:
        conn = duckdb.connect()


        #formated duckDB query strings for easy insertion 
        QUERY = '''SELECT * FROM {0} LIMIT {c} OFFSET {num}'''
        count_q= ''' SELECT COUNT(*) FROM {0} '''  # counts row of parquet very fast

        #duckDB queries with formated num from setup method
        count = conn.execute(count_q.format(f)).df().to_dict()['count_star()'][0] 
                 

        counter = 0

        while counter < count:
            conn.execute(QUERY.format(f,c=chunk,num=counter))
            counter+=chunk



        
