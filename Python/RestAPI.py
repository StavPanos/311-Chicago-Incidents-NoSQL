import flask
from flask import request, jsonify
from bson.objectid import ObjectId
from bson.json_util import dumps
import pymongo
import datetime


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
        return "Error: wrong arguments"

    list_cur = list(list(db.incidents.find({"_id": ObjectId(id)})))

    return dumps(list_cur, indent=2)


@app.route('/api/query/1', methods=['GET'])
def api_1():
    if 'date1' in request.args and 'date2' in request.args:
        date1 = str(request.args['date1'])
        date2 = str(request.args['date2'])
    else:
        return "Error: wrong arguments"

    return dumps(list(db.incidents.aggregate([
                            {"$match": {"creation_date": {"$gte": datetime.datetime.strptime(date1+" "+"00:00:00", '%y/%m/%d %H:%M:%S'), "$lte": datetime.datetime.strptime(date2+" "+"00:00:00", '%y/%m/%d %H:%M:%S')}}},
                            {"$group": {"_id": "$service_request_type", "total": {"$sum": 1}}},
                            {"$sort": {"total": -1}}
                            ])))


@app.route('/api/query/2', methods=['GET'])
def api_2():
    if 'date1' in request.args and 'date2' in request.args and 'req_type' in request.args:
        req_type = int(request.args['req_type'])
        date1 = str(request.args['date1'])
        date2 = str(request.args['date2'])
    else:
        return "Error: wrong arguments"

    return dumps(list(db.incidents.aggregate([
                           {"$match": {"service_request_type": req_type, "creation_date": {"$gte": datetime.datetime.strptime(date1+" "+"00:00:00", '%y/%m/%d %H:%M:%S'), "$lte": datetime.datetime.strptime(date2+" "+"00:00:00", '%y/%m/%d %H:%M:%S')}}},
                           {"$group": {"_id": "$creation_date", "total": {"$sum": 1}}},
                           {"$sort": {"total": -1}}
                           ])))


@app.route('/api/query/3', methods=['GET'])
def api_3():
    if 'date' in request.args:
        date = str(request.args['date'])
    else:
        return "Error: wrong arguments"

    return  dumps(list(db.incidents.aggregate([
                           {"$match": {"creation_date": datetime.datetime.strptime(date+" "+"00:00:00", '%y/%m/%d %H:%M:%S')}},
                           {"$group": {"_id": "$zip_code", "count": {"$sum": 1}}},
                           {"$sort": {"count": -1 }},
                           {"$limit": 3}
                           ])))


@app.route('/api/query/4', methods=['GET'])
def api_4():
    if 'req_type' in request.args:
        req_type = int(request.args['req_type'])
    else:
        return "Error: wrong arguments"

    return dumps(list(db.incidents.aggregate([
                       {"$match": {"service_request_type": req_type, "ward": {"$ne": 0}}},
                       {"$group": {"_id": "$ward", "count": {"$sum": 1}}},
                       {"$sort": {"count": 1}},
                       {"$limit": 3}
                       ])))


@app.route('/api/query/5', methods=['GET'])
def api_5():
    if 'date1' in request.args and 'date2' in request.args:
        date1 = str(request.args['date1'])
        date2 = str(request.args['date2'])
    else:
        return "Error: wrong arguments"

    return dumps(list(db.incidents.aggregate([
                           {"$match": {"creation_date": {"$gte": datetime.datetime.strptime(date1+" "+"00:00:00", '%y/%m/%d %H:%M:%S'), "$lt": datetime.datetime.strptime(date2+" "+"00:00:00", '%y/%m/%d %H:%M:%S')}}},
                           {"$group": {"_id": "null", "days": {"$avg": {"$divide": [{"$subtract": ["$completion_date", "$creation_date"]}, 86400000]}}}}
                           ])))


@app.route('/api/query/6', methods=['GET'])
def api_6():
    if 'date' in request.args and 'lat1' in request.args and 'lat2' in request.args and 'long1' in request.args and 'long2' in request.args:
        date = str(request.args['date'])
        lat1 = float(request.args['lat1'])
        lat2 = float(request.args['lat2'])
        long1 = float(request.args['long1'])
        long2 = float(request.args['long2'])
    else:
        return "Error: wrong arguments"

    return dumps(list(db.incidents.aggregate([
                           {"$match": { "creation_date": datetime.datetime.strptime(date+" "+"00:00:00", '%y/%m/%d %H:%M:%S'), "latitude": {"$gte": lat1 , "$lt": lat2}, "longitude": { "$gte": long1, "$lt": long2}}},
                           {"$group": { "_id": "$service_request_type", "requestCount": {"$sum": 1 }}},
                           {"$sort": { "requestCount": -1 } },
                           {"$limit": 1 }
                           ])))


@app.route('/api/query/7', methods=['GET'])
def api_7():
    if 'date' in request.args:
        date = str(request.args['date'])
    else:
        return "Error: wrong arguments"

    return dumps(list(db.incidents.aggregate([
                           {"$match": {"creation_date": datetime.datetime.strptime(date+" "+"00:00:00", '%y/%m/%d %H:%M:%S')}},
                           {"$sort": {"total_upvotes": -1}},
                           {"$limit": 50}
                           ])))


@app.route('/api/query/8', methods=['GET'])
def api_8():

    return dumps(list(db.user.aggregate([
                                {"$project": {"name": "$name", "upvotes_count": {"$size": {"$ifNull": ["$upvotes", []]}}}},
                                {"$sort": {"upvotes_count": -1}},
                                {"$limit": 50}
                                ])))


@app.route('/api/query/9', methods=['GET'])
def api_9():

    return dumps(list(db.user.aggregate([
                                {"$unwind": "$upvotes"},
                                {"$group": {"_id": "$upvotes.ward", "upvoteCount": {"$sum": 1}}},
                                {"$sort": {"upvoteCount": -1}}
                            ])))


@app.route('/api/query/11', methods=['GET'])
def api_11():
    if 'name' in request.args:
        name = str(request.args['name'])

    return dumps(list(db.user.aggregate([
                   {"$match": {"name": name}},
                   {"$unwind":"$upvotes"},
                   {"$group": {"_id": "$upvotes.ward"}},
                   {"$sort": {"_id": -1 }}
                   ])))


app.run()
