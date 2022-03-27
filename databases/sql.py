from configparser import ConfigParser
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import json


class Postgres():
    # https://www.postgresqltutorial.com/postgresql-python/connect/

    def config(self, filename='databases/database.ini', section='postgresql'):
        parser = ConfigParser()
        # read config file
        parser.read(filename)


        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db

    def write_df(self, df, table, schema):
          
        conn = self.get_connection()
        df.to_sql(table, schema=schema, con=conn, if_exists='append', index=False)
        
        conn.close()

    def read_db(self, query):
    
        conn = self.get_connection()
        df = pd.read_sql(query, conn, coerce_float=False)
        conn.close()

        return df

    def get_connection(self):

        try:
            params = self.config()
            conn_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"

            engine = create_engine(conn_string)
            connection = engine.connect()

            return connection
        except:
            return print("Connection failed.")



if __name__ == '__main__':
    pass


    