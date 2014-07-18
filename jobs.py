#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
from datetime import datetime
from urllib2 import HTTPError
from math import ceil
import linkedin as lk
import mechanize, json, time, heapq, re, couchdb, csv, urllib2, random

############################# 
## CONFIGURATION PARAMETER ##
#############################

config = lk.config("config.json")

''' Exact Search Word 
      (1) Data Engineer; 
      (2) Data Science; 
      (3) Data Scientist; 
      (4) Data Modeler; 
      (5) Data Architect;
      (6) Statistical Analyst
      (7) Machine Learning
    Multiple Region Search
    (1) Data Mining (CareFusion, Microsoft, Amazon, Rest)
    (2) Big Data (Developer, IBM, Deloitte, CyberCoders, Teradata, Hadoop, Senior, microsoft)

    (1) Data Analyst

    (3) Business Intelligence
'''


KEYWORDS = ''

# CouchDB Paramter
DBADDRESS = config["database"]["couchdb_address"]
JOBDBNAME = config["database"]["job_dbname"]
LOGDBNAME = config["database"]["log_dbname"]

def main():

    users = config["linkedinAccount"]
    for user in users:
        webscrape(user, users[user], False, True)

    
    
def webscrape(user, pw, loopzip=True, verbose=False):
    """ db is the database used to store the job scraping result
        log is the database used to store the status of the web scraping
    """

    # intial the browser object
    browser = lk.ExtendedBrowser()
    browser.init()
    browser.login("linkedin", user, pw, True)

    # Linkedin only make the first 1000 result available to users
    # search keyword with different zipcode to get around this limitation
    if loopzip:
        zipList = getZipcodeList(browser.logdb)
        zipListLen = len(zipList)
        random.seed(2)
        zipListLen = random.sample(zipList,zipListLen)
    else:
        zipList = ["00000"]

    for zipcode in zipList:

        jobids_dict = lk.getJobIDByKeyword(browser, KEYWORDS, zipcode, 25, True)
        
        jobids = []
        for pageNum in jobids_dict: jobids.extend(jobids_dict[pageNum])

        # obtain the complete info for each given jobid
        for jobid in jobids:
            if jobid not in browser.scrapedoc["finish"]:
                result = lk.getJobInfo(jobid)
                if verbose: print "\tAdd Job: ", unicode(result["title"][:30]).encode("utf-8")
                browser.jobdb.save(result)
                browser.scrapedoc["finish"].append(jobid)
                browser.updateLog()
                 
    browser.close()
    print "Script Reach its End"

def checkFinish(logdb, scrape_doc,jobid):
    return ("finish" in scrape_doc and 
            jobid in scrape_doc["finish"])

def updateFinish(logdb, scrape_doc,jobid):
    if "finish" not in scrape_doc: scrape_doc["finish"] = []
    if jobid not in scrape_doc["finish"]: scrape_doc["finish"].append(jobid) 
    logdb.save(scrape_doc)

def checkQueue(logdb, scrape_doc, kw, zipcode): 
    return ("queue" in scrape_doc and 
            kw in scrape_doc["queue"] and
            zipcode in scrape_doc["queue"][kw])

def updateQueue(logdb, scrape_doc, kw, zipcode, jobIDList):
    if "queue" not in scrape_doc: scrape_doc["queue"] = {}
    if kw not in scrape_doc["queue"]: scrape_doc["queue"][kw] = {}
    scrape_doc["queue"][kw][zipcode] = jobIDList
    logdb.save(scrape_doc)

def getZipcodeList(logdb):
    map_fun = """
    function(doc) {
        if (doc.doctype == "search_area" && 
            doc.isValid == true && 
            doc.resultCount > 5) {
            emit([doc.state,doc.county], 
                 [doc.median_zipcode,doc.median_zipcodes]);
        }
    } """

    zipcodeList = []
    for row in logdb.query(map_fun):
        # for some less populous city, only one zipcode will be searched
        # for those more populous city, a list of zipcode will be searched
        median_zipcode, median_zipcodes = row.value
        if median_zipcodes == None: zipcodeList.append(median_zipcode)
        else: zipcodeList.extend(median_zipcodes)
        
    return zipcodeList



if __name__ == "__main__": main()

