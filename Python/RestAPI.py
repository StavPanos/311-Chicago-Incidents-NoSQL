import flask
from flask import request, jsonify
from bson.objectid import ObjectId
from bson.json_util import dumps
import pymongo


app = flask.Flask(__name__)
app.config["DEBUG"] = True

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["M149"]


@app.route('/', methods=['GET'])
def home():
    return "<h1>311-Chicago Incidents REST API</h1><p>Serving Flask App</p>"


@app.route('/api/query/id', methods=['GET'])
def api_id():
    if 'id' in request.args:
        id = str(request.args['id'])
    else:
        return "Error: No dates provided. Please specify the 2 dates."

    list_cur = list(list(db.incidents.find({"_id": ObjectId(id)})))

    return dumps(list_cur, indent=2)


@app.route('/api/query/1', methods=['GET'])
def api_1():
    if 'date1' in request.args:
        date1 = str(request.args['date1'])
        # date2 = str(request.args['date2'])
        print(date1, date1)
    else:
        return "Error: No dates provided. Please specify the 2 dates."

    results = []

    results = db.incidents.aggregate([
                            {"$match": {"creation_date": {"$gte": {"$toDate": date1}, "$lte": {"$toDate": date1}}}},
                            {"$group": {"_id": "$service_request_type", "total": {"$sum": 1}}},
                            {"$sort": {"total": -1}}
                            ])

    return jsonify(results)


@app.route('/api/query/1', methods=['GET'])
def api_2():
    if 'date1' in request.args:
        date1 = str(request.args['date1'])
        # date2 = str(request.args['date2'])
        print(date1, date1)
    else:
        return "Error: No dates provided. Please specify the 2 dates."

    results = []

    results = db.incidents.aggregate([
                            {"$match": {"creation_date": {"$gte": {"$toDate": date1}, "$lte": {"$toDate": date1}}}},
                            {"$group": {"_id": "$service_request_type", "total": {"$sum": 1}}},
                            {"$sort": {"total": -1}}
                            ])

    return jsonify(results)


@app.route('/api/query/1', methods=['GET'])
def api_3():
    if 'date1' in request.args:
        date1 = str(request.args['date1'])
        # date2 = str(request.args['date2'])
        print(date1, date1)
    else:
        return "Error: No dates provided. Please specify the 2 dates."

    results = []

    results = db.incidents.aggregate([
                            {"$match": {"creation_date": {"$gte": {"$toDate": date1}, "$lte": {"$toDate": date1}}}},
                            {"$group": {"_id": "$service_request_type", "total": {"$sum": 1}}},
                            {"$sort": {"total": -1}}
                            ])

    return jsonify(results)


@app.route('/api/query/1', methods=['GET'])
def api_4():
    if 'date1' in request.args:
        date1 = str(request.args['date1'])
        # date2 = str(request.args['date2'])
        print(date1, date1)
    else:
        return "Error: No dates provided. Please specify the 2 dates."

    results = []

    results = db.incidents.aggregate([
                            {"$match": {"creation_date": {"$gte": {"$toDate": date1}, "$lte": {"$toDate": date1}}}},
                            {"$group": {"_id": "$service_request_type", "total": {"$sum": 1}}},
                            {"$sort": {"total": -1}}
                            ])

    return jsonify(results)


app.run()
