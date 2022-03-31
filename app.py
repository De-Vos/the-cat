import flask
from flask_restful import Api

import databases.sql as sql
import databases.no_sql as no_sql


app = flask.Flask(__name__)
api = Api(app)

# TODO: 
# - Improve formatting: automatic float conversion on int types when
#   reading from database.
    
@app.route('/reservations-sql', methods=['GET'])
def reservations_sql():
    
    query = open("databases/sql/select_reservations.sql", 
                mode='r', 
                encoding='utf-8-sig').read()
    
    psql = sql.Postgres()
    df = psql.read_db(query)
    
    format = flask.request.args.get('format') 

    return parse_format(df, format)


@app.route('/reservations-location-sql', methods=['GET'])
def reservations_locations_sql_csv():
    query = open("databases/sql/reservations_join_locations.sql", 
                mode='r', 
                encoding='utf-8-sig').read() 

    psql = sql.Postgres()
    df = psql.read_db(query)

    format = flask.request.args.get('format') 

    return parse_format(df, format)


@app.route('/reservations-nosql', methods=['GET'])
def reservations_nosql():

    mongo = no_sql.MongoDB()
    df = mongo.get_df(db='felyx', collection="reservations")
    
    format = flask.request.args.get('format') 

    return parse_format(df, format)


def parse_format(df, format):

    if format == 'json':
        return flask.jsonify(df.to_dict(orient="records"))

    elif format == 'csv':
        return flask.Response(
            df.set_index('id').to_csv(),
            mimetype="text/csv",
            headers={"Content-disposition":
            "attachment; filename=filename.csv"})
    
    else:
        return flask.jsonify(
            {"response": "invalid format"})



if __name__ == '__main__':
    app.run(debug=True)



