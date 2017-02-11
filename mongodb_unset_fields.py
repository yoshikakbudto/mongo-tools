#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
removes meta field from documents with ObjectIds given in OBJ_IDS_FIE

the list generated based on the log of 
    mongo --eval "db.getMongo().setSlaveOk(); db.upgradeCheckAllDBs();" db06av-ru.psc/admin > /tmp/upgradecheckalldbs.txt
sed -nE 's/(.*ObjectId..)([[:alnum:]]+)(.*)/\2/p' /tmp/upgradecheckalldbs_builduser.txt

"""

import os
import pprint
from pymongo import MongoClient
from bson.objectid import ObjectId

MONGO_HOST = 'db04av-ru.psc'
MONGO_PORT = 27017
MONGO_DB = 'cosmosim_pubtest'
MONGO_COLLECTION = 'freetopayQueue'
OBJ_IDS_FILE = 'object_ids.lst'

of = os.path.dirname(os.path.realpath(__file__)) + r'/' + OBJ_IDS_FILE
objids=[]
with open(of, 'r') as f:
    for line in f.xreadlines():
        objids.append(line.strip())

with MongoClient(MONGO_HOST, MONGO_PORT, maxPoolSize=3) as c:
    db = c[MONGO_DB]
    col = db[MONGO_COLLECTION]
    # pprint.pprint(col.find_one({"_id" : ObjectId("558d77ce2f410a101e293a5a")}))
    for objid in objids:    
        print "removing meta field from doc id %s" % objid
        col.update({'_id': ObjectId(objid)}, {'$unset':{'meta':1}})
# print(objids)

