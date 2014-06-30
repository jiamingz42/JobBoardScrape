#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import datetime
import urllib2, re, json

def main():
    getJobInfo("12880365", verbose=False)

def connectdb(couchdb, dbname):
    """ connect to the db if existed or create it """

    # check whether the db exists
    isExist = False
    for database in couchdb: 
        if database == dbname: isExist = True

    # connect the database or create it if not exist
    if isExist == True:
        database = couchdb[dbname]
    else:
        couch.create(dbname)
        database = couchdb[dbname]

    return database
 
def getCompanyInfo(companyid, verbose=False):

    result = {"companyid"   : companyid,
              "companyURL"  : "https://www.linkedin.com/company/%s" % companyid}

    html = urllib2.urlopen(result["companyURL"])
    soup = BeautifulSoup(html)

    getName    = lambda soup: soup.find("span", itemprop="name").text.strip()
    getInd     = lambda soup: soup.find("li", class_="industry").find("p").text.strip()
    getType    = lambda soup: soup.find("li", class_="type").find("p").text.strip()
    getSize    = lambda soup: soup.find("li", class_="company-size").find("p").text.strip()
    getFounded = lambda soup: soup.find("li", class_="founded").find("p").text.strip()
    getSp      = lambda soup: soup.find("div", class_="specialties").find("p").text.replace("\n","").split(",")
    
    funList = [("Name"          ,getName    ),
               ("Industry"      ,getInd     ),
               ("Type"          ,getType    ),
               ("Size"          ,getSize    ),
               ("Founded"       ,getFounded ),
               ("Specifities"   ,getSp      )]

    for (key, fun) in funList:
        try:
            result[key] = fun(soup)
        except AttributeError:
            result[key] = None
            warning = "Company %s Can't Find %s Info" % (companyid, key)
            if verbose: print warning

    message = "\tCompany: %s"    
    if verbose: print message % (result["Name"])

    return result


def getJobInfo(jobid, verbose=False):

    result = { "jobid"       : jobid,
               "source"      : "Linkedin",
               "scrapetime"  : str(datetime.datetime.now()),
               "jobURL"      : "https://www.linkedin.com/jobs2/view/%s" % jobid }

    html = urllib2.urlopen(result["jobURL"])
    soup = BeautifulSoup(html)

    def getName(soup):
        name = soup.find("h1", itemprop="title").text
        return name.strip()
    def getCompany(soup):
        company = soup.find("a", class_="company").text
        return company.strip()
    def getCompanyID(soup):
        codeString  = soup.find("code", id="biz_feed-content").string
        codeJSON    = json.loads(codeString)
        companyID   = codeJSON["content"]["feed"]["currentActor"]["companyId"]
        return unicode(companyID)
    def getLocation(soup):
        location = soup.find("span", itemprop="jobLocation").text
        return location.strip()
    def getDate(soup):
        deltaText = soup.find("div", class_="posted").text
        deltaNum = int(re.compile("[0-9]+").findall(deltaText)[0])
        if "day" in deltaText:
            deltaDate = datetime.timedelta(days=deltaNum)
        elif "hour" in deltaText:
            deltaDate = datetime.timedelta(hours=deltaNum)
        else:
            print "Unknown Date Format"
            raise Exception 
        now = datetime.datetime.today()
        pos = now - deltaDate
        return {"year":2000, "month":1, "day":1}
    def getJobd(soup):
        return unicode(soup.find("div",class_="description-module container"))

    funList = [("title",        getName         ),
               ("company",      getCompany      ),
               ("companyid",    getCompanyID    ),
               ("location",     getLocation     ),
               ("date",         getDate         ),
               ("description",  getJobd         )]


    for (key, fun) in funList:
        result[key] = fun(soup)


    message = "\tTitle: %s, Company: %s"    
    if verbose: print message % (result["title"],result["company"])
                         
    return result

def removeTrailing(mydict):
    """ remove the trailing space in the value of a dictionary """
    for key in mydict: 
        value = mydict[key]
        if type(value) == str:
            mydict[key] = value.strip()

if __name__ == "__main__": main()


