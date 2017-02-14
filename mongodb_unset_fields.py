#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
removes meta field from documents with ObjectIds given in OBJ_IDS_FIE

the list generated based on the log of 
    mongo --eval "db.getMongo().setSlaveOk(); db.upgradeCheckAllDBs();" db06av-ru.psc/admin > /tmp/upgradecheckalldbs.txt
sed -nE 's/(.*ObjectId..)([[:alnum:]]+)(.*)/\2/p' /tmp/upgradecheckalldbs_builduser.txt

"""

import os
# import pprint
import sys
import time

from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
    FileTransferSpeed, FormatLabel, Percentage, \
    ProgressBar, ReverseBar, RotatingMarker, \
    SimpleProgress, Timer

from pymongo import MongoClient
from bson.objectid import ObjectId

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'cosmosim'
MONGO_COLLECTION = 'cosmosim.reservedAdQueue'
OBJ_IDS_FILE = 'object_ids.lst'

of = os.path.dirname(os.path.realpath(__file__)) + r'/' + OBJ_IDS_FILE
objids=[]
widgets = [Percentage(),
           ' ', Bar(),
           ' ', ETA(),]

with open(of, 'r') as f:
    for line in f.xreadlines():
        objids.append(line.strip())

pbar = ProgressBar(widgets=widgets, maxval=len(objids))

with MongoClient(MONGO_HOST, MONGO_PORT, maxPoolSize=3) as c:
    db = c[MONGO_DB]
    col = db[MONGO_COLLECTION]
    # pprint.pprint(col.find_one({"_id" : ObjectId("558d77ce2f410a101e293a5a")}))
    pbar.start()
    pbar_counter = 0
    for objid in objids:    
        pbar_counter = pbar_counter + 1
        # print "removing meta field from doc id %s" % objid
        col.update({'_id': ObjectId(objid)}, {'$unset':{'meta':1}})
        pbar.update(pbar_counter)
    pbar.finish()

# print(objids)

