import duckdb
import oakvar
from oakvar import BaseConverter



class Converter(BaseConverter):    
    def check_format(self,file):
        return file.name.endswith('.parquet') or file.name.endswith('.parquet.gz')
    
    def convert_file(self,file):
        conn = duckdb.connect()
        #look up the amount of rows 
        row_q = 'row_group_num_rows'
        rows = conn.execute(f'SELECT {row_q} FROM parquet_metadata("{file}")').df().to_dict()
        num_rows = rows[row_q][0]

        #loop through chunk of size 1000 within the amount of rows 
        # want to send result of each row while inside loop

        post_tup = {} # loop through this after chunk
        start = 0
        chunk = 1000
        end = chunk
        
        while start < num_rows:
            print(start)
            QUERY = f'SELECT * FROM parquet_scan("{file}") LIMIT {chunk} OFFSET {start}'
            count = conn.execute(QUERY).df().to_dict('index')
            for item in list(count.keys()):
                count.update({item+start:count[item]})
            for line_no in list(count.keys()):
                yield line_no, self.convert_line(count[line_no])

            post_tup.update(count)
            start+=chunk
            end += chunk
            yield post_tup 
        #each loop within chunk goes back to master converter (this has to be a single row)

