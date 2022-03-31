import redis
from pymongo import MongoClient
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

        r = self.get_connection(db)
        
        df_dict = df.set_index(df.id).T.to_dict()
        
        for key, value in df_dict.items():
            r.set(str(key), str(value))


    def get_df(self, db):
        
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


class MongoDB():

    def config(self, filename='databases/database.ini', section='mongodb'):

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
        
        return MongoClient(host=params['host'],
            port=int(params['port']),
            username=params['user'],
            password=params['password'],
            authSource=params['auth_database'])


    def set_df(self, df, db, collection):

        m = self.get_connection(db)
        db = m[db]
        col = db[collection]

        df = df.rename(columns={"id": "_id"})
        df_dict = df.set_index(df._id).to_dict(orient='records')

        col.insert_many(df_dict)


    def get_df(self, db, collection):
        
        m = self.get_connection(db)
        db = m[db]
        col = db[collection]

        data = col.find()

        df = pd.DataFrame.from_records(data)
        df = df.rename(columns={"_id": "id"})
        
        return df


if __name__ == '__main__':

    pass
