from faker import Faker
from bson.objectid import ObjectId
import pymongo
import random

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["M149"]

fake = Faker()


def gen_phone():
    first = str(random.randint(100, 999))
    second = str(random.randint(1, 888)).zfill(3)
    last = (str(random.randint(1, 9998)).zfill(4))

    return '{}-{}-{}'.format(first, second, last)


def create_upvotes(_fake):
    for x in range(500):

        name = _fake.name()
        address = _fake.address()

        upvotes = []

        result = db.user.insert_one(
            {
                'name': name,
                "telephone_nr": gen_phone(),
                'address': address
            }
        )

        _i = 0

        for i in range(random.randint(1, 200)):

            # find random incident
            cursor = db.incidents.aggregate([{'$sample': {'size': 1}}])

            # cast cursor to list
            incident = list(cursor)

            # check total upvotes
            if int(incident[0]['total_upvotes']) >= 3:
                _i += 1
                continue

            upvotes.append({'_id': incident[0]['_id'],
                            'ward': int(incident[0]['ward'])
                            })

            # update incident document
            db.incidents.update_one({'_id': ObjectId(incident[0]['_id'])},
                               {'$set': {'upvotes.' + str(incident[0]['total_upvotes']):
                                             {'_id': result.inserted_id,
                                              'name': name}
                                         },
                                '$inc': {'total_upvotes': 1}
                                })

        # update user document
        db.user.update_one({'_id': result.inserted_id}, {'$set': {'upvotes': upvotes}})


create_upvotes(fake)