import redis
from configparser import ConfigParser
import pandas as pd
import json


class Redis():

    def config(self, filename='databases/database.ini', section='redis'):

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


    def get_connection(self, db):

        params = self.config()

        return redis.Redis(host=params['host'],
            port=params['port'], 
            db=params[db])


    def set_df(self, df, db):
        # TODO: improve format of storage

        r = self.get_connection(db)
        
        df_dict = df.set_index(df.id).T.to_dict()
        
        for key, value in df_dict.items():
            r.set(str(key), str(value))

    def get_df(self, db):
        # TODO: improve efficiency of extraction
        
        r = self.get_connection(db)
        keys = r.keys('*')
        
        output = pd.DataFrame()
        for key in keys:
            val = r.get(key).decode("utf-8")
            val = val.replace("'", '"').replace('None', '"-"')
            js = json.loads(val)
            output = output.append(js, ignore_index=True)

        output = output.replace({'-': None})

        return output


if __name__ == '__main__':
    pass