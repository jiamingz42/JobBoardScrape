#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2, json,couchdb
import linkedin as lk
from bs4 import BeautifulSoup

# CouchDB Paramter
DBADDRESS = 'http://127.0.0.1:5984'
DBNAME = "company" 

def main():
    # add name later

    # connect the server/database
    couch = couchdb.Server(DBADDRESS)
    database = lk.connectdb(couch, DBNAME)
    
    companys = ["1035"]

    for companyid in companys:
        result = lk.searchCompanyInLinkedin(companyid)
        if len(database.query(map_fun("Linkedin", companyid))) == 0:
            database.save(result)
        print result["Name"]


map_fun = lambda source, companyid: """ 
            function(doc) {
                if (doc.source = "%s" && doc.companyid == "%s") {
                    emit(doc.title, null);
                }
            } """ % (source, companyid)



main()









