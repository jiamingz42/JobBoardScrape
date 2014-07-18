#!/usr/bin/env python
# -*- coding: utf-8 -*-

from geopy.geocoders import GoogleV3
import linkedin as lk
import couchdb, time


############################# 
## CONFIGURATION PARAMETER ##
#############################

CONFIG = lk.config("config.json")

DBADDRESS = CONFIG["database"]["couchdb_address"]
JOBDBNAME = CONFIG["database"]["job_dbname"]
LOGDBNAME = CONFIG["database"]["log_dbname"]
MAPDBNAME = CONFIG["database"]["mapping_dbname"]

def main():
    
    # connect the server
    couch = couchdb.Server(DBADDRESS)
    jobdb = lk.connectdb(couch, JOBDBNAME)
    mapdb = lk.connectdb(couch, MAPDBNAME)

    # obtain unique address from jobdb
    uniAddrViewFromJob = getUniqueAdress(jobdb)
    uniAddrSetFromJob = lk.viewToSet(uniAddrViewFromJob)

    # obtain unique address from mapdb
    docid = getLocationMapping(mapdb)
    doc = mapdb[docid]
    uniAddrSetFromMap = set()
    for rawLocation in doc["mapping"]: uniAddrSetFromMap.add(rawLocation)
    
    # address that occur in jobdb but not in mapdb need geocoded
    queue = uniAddrSetFromJob.difference(uniAddrSetFromMap)
    print "%d Address to be Processed" % len(queue)
    for i, rawLocation in enumerate(queue):
        geocoded = lk.parseLocationRaw(rawLocation)
        doc["mapping"][rawLocation] = geocoded
        mapdb.save(doc)
        time.sleep(2)
        print "\t",geocoded
                

def getLocationMapping(mapdb):
    # assume the db only has one doc with doctype as "location_mapping"

    map_fun = """ function (doc){
        if (doc.doctype == "location_mapping") {
            emit(null, doc);
        }
    }
    """
    view = mapdb.query(map_fun)
    for row in view: 
        docid = row.id
        break
    return docid

def getUniqueAdress(jobdb):
    map_fun = """ function (doc){
        if (doc.type == "job" && doc.location) {
            emit(doc.location, 1);
        }
    }
    """
    reduce_fun = """ function (key, values){
        return sum(values);
    }
    """

    return jobdb.query(map_fun, reduce_fun=reduce_fun, group_level=1)


if __name__ == "__main__": main()