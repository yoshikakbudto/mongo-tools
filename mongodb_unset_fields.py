#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
removes meta field from documents with ObjectIds given in OBJ_IDS_FIE

the list generated based on the log of mongo --eval "db.getMongo().setSlaveOk(); db.upgradeCheckAllDBs();" db06av-ru.psc/admin > /tmp/upgradecheckalldbs.txt
sed -nE 's/(.*ObjectId..)([[:alnum:]]+)(.*)/\2/p' /tmp/upgradecheckalldbs_builduser.txt

"""

import os
import pymongo

OBJ_IDS_FILE = 'object_ids.lst'

of = os.path.dirname(os.path.realpath(__file__)) + r'/' + OBJ_IDS_FILE
objids=[]
with open(of, 'r') as f:
    for line in f.xreadlines():
        objids.append(line.strip())

# print(objids)

