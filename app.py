import flask
from flask_restful import Api
import json

import databases.sql as sql
import databases.no_sql as no_sql


app = flask.Flask(__name__)
api = Api(app)

# TODO: 
# - Improve formatting: automatic float conversion on int types when
#   reading from database.
# - Improve formatting: add "id" value on JSON export
    
@app.route('/reservations-sql.json', methods=['GET'])
def reservations_sql_json():
    query = open("databases/sql/select_reservations.sql", 
                mode='r', 
                encoding='utf-8-sig').read()

    psql = sql.Postgres()
    df = psql.read_db(query)
    df = df.set_index('id')
    
    return flask.jsonify(df.to_dict(orient="index"))

@app.route('/reservations-sql.csv', methods=['GET'])
def reservations_sql_csv():
    query = open("databases/sql/select_reservations.sql", 
                mode='r', 
                encoding='utf-8-sig').read()

    psql = sql.Postgres()
    df = psql.read_db(query)
    df = df.set_index('id')
    
    return flask.Response(
        df.to_csv(),
        mimetype="text/csv",
        headers={"Content-disposition":
        "attachment; filename=filename.csv"})

@app.route('/reservations-location-sql.json', methods=['GET'])
def reservations_locations_sql_json():
    query = open("databases/sql/reservations_join_locations.sql", 
                mode='r', 
                encoding='utf-8-sig').read()

    psql = sql.Postgres()
    df = psql.read_db(query)
    df = df.set_index('id')
    return  flask.jsonify(df.to_dict(orient="index"))

@app.route('/reservations-location-sql.csv', methods=['GET'])
def reservations_locations_sql_csv():
    query = open("databases/sql/reservations_join_locations.sql", 
                mode='r', 
                encoding='utf-8-sig').read() 

    psql = sql.Postgres()
    df = psql.read_db(query)
    df = df.set_index('id')

    return flask.Response(
        df.to_csv(),
        mimetype="text/csv",
        headers={"Content-disposition":
        "attachment; filename=filename.csv"})

@app.route('/reservations-nosql.json', methods=['GET'])
def reservations_nosql_json():
    psql = no_sql.Redis()
    df = psql.get_df(13)
    df = df.set_index('id')
    
    return  flask.jsonify(df.to_dict(orient="index"))

@app.route('/reservations-nosql.csv', methods=['GET'])
def reservations_nosql_csv():
    psql = no_sql.Redis()
    df = psql.get_df(13)
    df = df.set_index('id')
    
    return flask.Response(
        df.to_csv(),
        mimetype="text/csv",
        headers={"Content-disposition":
        "attachment; filename=filename.csv"})


if __name__ == '__main__':
    app.run(debug=True)
