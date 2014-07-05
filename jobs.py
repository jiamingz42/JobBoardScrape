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

# Linkedin Log in Parameter
KEYWORDS = "Data Analyst"
LOCATIONS = ["California","CA"]
USERNAME = "student4561@163.com" 
PASSWORD = "VoHa6DBAgPgf2h"

# CouchDB Paramter
DBADDRESS = 'http://127.0.0.1:5984'

''' each complete scrape job will be store the following info into a doc
{
    doctype: "scrape",
    scrape_id: "00000001",
    start_datetime: '2014-07-04 17:12:50.226416' # str(datetime.today())
    end_datetime: '2014-07-04 17:12:50.226416' # str(datetime.today())
    queue = {
                "data analyst": {
                                    "15213":[job1_id,job2_id],
                                    "15214":[job1_id,job2_id],
                                }
                "data science": {"15213":[job1_id,job2_id]}
            }
    finish = [job1_id, jobd2_id, jobd3_id ...]
}
'''


def main():
    
    # connect the server
    couch = couchdb.Server(DBADDRESS)
    db = lk.connectdb(couch, "analytics2")
    log = lk.connectdb(couch, "log")

    # username and password
    users = {"student23@163.com":"jz4kFZRBi4",
             "student4561@163.com":"VoHa6DBAgPgf2h",
             "student4562@163.com":"ZTYtQBiYu9fDR4",
             "student4563@163.com":"ramj63JvVy"}

    for user in users:
        try: webscrape(user, users[user], db, log, "0001", True)
        except: pass
        
    
def webscrape(user, pw, db, log, scrapeid="0001", verbose=False):
    """ db is the database used to store the job scraping result
        log is the database used to store the status of the web scraping
    """
    # scrape_doc records the scrape job status
    scrape_docid = getScrapeDoc(log, scrapeid)
    scrape_doc = log[scrape_docid]

    # intial the browser object
    browser = ExtendedBrowser()
    browser.init()
    browser.login("linkedin", user, pw, True)

    # Linkedin only make the first 1000 result available to users
    # search keyword with different zipcode to get around this limitation
    zipList = getZipcodeList(log)
    zipListLen = len(zipList)
    zipList = random.sample(zipList,zipListLen)

    for zipcode in zipList:
        print "%.2f%%" % (float(zipList.index(zipcode))/zipListLen * 100)
        # checkQueue return false 
        # if the current combination of kw and zipcode is not searched
        if checkQueue(log, scrape_doc, KEYWORDS, zipcode) == False:
            # if verbose: print "(%s,%s) has not been searched" % (KEYWORDS, zipcode)
            jobIdList = lk.getJobIDByKeyword(browser, KEYWORDS, zipcode, 50, True)  
            updateQueue(log, scrape_doc, KEYWORDS, zipcode, jobIdList)
        else:
            # if verbose: print "(%s,%s) has been searched" % (KEYWORDS, zipcode)
            jobIdList = scrape_doc["queue"][KEYWORDS][zipcode]

        # obtain the complete info for each given jobid
        for jobid in jobIdList:
            if checkFinish(log, scrape_doc,jobid) == False:
                result = lk.getJobInfo(jobid)
                updateFinish(log, scrape_doc, jobid)
                db.save(result)
                if verbose: print "\tAdd Job: ", unicode(result["title"][:50]).encode("utf-8")
                time.sleep(1)

                 
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
    
def getScrapeDoc(logdb, scrapeid):

    map_fun = lambda scrapeid:"""
    function(doc) {
        if (doc.doctype == "scrape" && doc.scrape_id == "%s") {
            emit(doc.scrapeid, doc);
        }
    } """ % scrapeid

    view = logdb.query(map_fun(scrapeid))
    viewLen = len(view)
    
    # if the input scrapeid does not exist
    if viewLen == 0:
        # create one
        doc = {"doctype": "scrape", 
               "scrape_id": str(scrapeid),
               "start_datetime": str(datetime.today())}
        logdb.save(doc)

        # update the view
        view = logdb.query(map_fun(scrapeid))
        viewLen = len(view)

    if viewLen == 1:
        for row in view: doc = row
    else:
        raise Exception("Duplicate ScrapeId")

    return doc.id

class ExtendedBrowser(mechanize.Browser):
    def init(self):
        self.set_handle_robots( False )
        self.addheaders = [('User-agent', 'Firefox')]

    def login(self, website, user, password, shouldPrint=False):
        sites = {"linkedin": "http://www.linkedin.com",
                 "facebook": "http://www.facebook.com"}

        URL = sites[website]
        
        self.open(URL)
        if shouldPrint:
            print "Open URL: %s" % URL
        
        self.select_form(nr = 0)
        self.form['session_key'] = user 
        self.form['session_password'] = password
        self.submit()

        if shouldPrint:
            print "Login As %s (Title: %s)" % (user, self.title())

    def get_data(self,url,goback=False):
        resp = self.open(url)
        html = resp.get_data()

        if goback: self.back()

        return html



if __name__ == "__main__": main()

