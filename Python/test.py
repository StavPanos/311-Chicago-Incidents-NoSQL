from bson.objectid import ObjectId
from bson.json_util import dumps
import pymongo
import datetime


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["M149"]


print(list(db.incidents.aggregate([
                       {"$match": {"service_request_type": 2, "ward": {"$ne": 0}}},
                       {"$group": {"_id": "$ward", "count": {"$sum": 1}}},
                       {"$sort": {"count": 1}},
                       {"$limit": 3}
                       ])))