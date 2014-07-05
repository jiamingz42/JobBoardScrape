#!/usr/bin/env python
# -*- coding: utf-8 -*-

import couchdb, time
import linkedin as lk

############################# 
## CONFIGURATION PARAMETER ##
#############################

config = lk.config("config.json")

DBADDRESS = config["database"]["couchdb_address"]
JOBDBNAME = config["database"]["job_dbname"]
COMDBNAME = config["database"]["company_dbname"]
LOGDBNAME = config["database"]["log_dbname"]

def main():

    # connect the server/database
    couch = couchdb.Server(DBADDRESS)
    jobdb = lk.connectdb(couch, JOBDBNAME)
    comdb = lk.connectdb(couch, COMDBNAME)
    
    # obtain unique compnayid from job database
    uniComViewFromJob = getUniqueCompany(jobdb)
    uniComSetFromJob = viewToSet(uniComViewFromJob)

    # obtain unique compnayid from company database
    uniComViewFromCom = getUniqueCompany(comdb)
    uniComSetFromCom = viewToSet(uniComViewFromCom)
    
    # scrape company that occur in job db but not in company db
    queue = uniComSetFromJob.difference(uniComSetFromCom)
    for companyid in queue:
        comInfo = lk.getCompanyInfo(companyid)
        comdb.save(comInfo)
        print comInfo["Name"]
        time.sleep(0.5)
    

def viewToSet(view):
        result = set()
        for row in view:
            key = row.key
            result.add(key)
        return result  

def getUniqueCompany(jobdb):
    map_fun = """ function(doc) {
        if (doc.companyid) {
            emit(doc.companyid, 1);
        }
    } """
    reduce_fun = """ function(key, values){
        return sum(values);
    } """
    view = jobdb.query(map_fun, reduce_fun=reduce_fun, group_level=1)
    return view


if __name__ == "__main__": main()









