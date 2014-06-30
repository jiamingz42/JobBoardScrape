#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
from datetime import datetime
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
    
    funList = [("Name",         getName     ),
               ("Industry",     getInd      ),
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

    return result


def getJobInfo(jobid, verbose=False):

    result = { "jobid"       : jobid,
               "source"      : "Linkedin",
               "scrapetime"  : str(datetime.now()),
               "jobURL"      : "https://www.linkedin.com/jobs2/view/%s" % jobid }

    html = urllib2.urlopen(result["jobURL"])
    soup = BeautifulSoup(html)

    def getName(soup):
        return soup.find("h1", itemprop="title").text
    def getCompany(soup):
        return soup.find("a", class_="company").text
    def getCompanyID(soup):
        codeString  = soup.find("code", id="biz_feed-content").string
        codeJSON    = json.loads(codeString)
        companyID   = codeJSON["content"]["feed"]["currentActor"]["companyId"]
        return unicode(companyID)
    def getLocation(soup):
        return soup.find("span", itemprop="jobLocation").text
    def getDate(soup):
        posted = soup.find("div", class_="posted").text
        number = re.compile("[0-9]+").findall(posted)[0]
        if "day" in posted:
            pass   
        elif "hour" in posted:
            pass
        else:
            return None
    def getJobd(soup):
        return unicode(soup.find("div",class_="description-module container"))

    funList = [("title",        getName         ),
               ("company",      getCompany      ),
               ("companyid",    getCompanyID    ),
               ("location",     getLocation     ),
               ("date",         getDate         ),
               ("description",  getJobd         )]

    getDate(soup)

    for (key, fun) in funList:
        result[key] = fun(soup).strip()


    # fh = open("debug.html", "w")
    # fh.write(unicode(soup.prettify()).encode("utf-8"))
    
    # search = soup.find("div", class_="posted").text
    # date = re.compile("Posted ([0-9]*) days ago").findall(search)[0]
    # # Posted 4 hours ago
    # print datetime.now() - date

    # # result["date"] = jobInfo["job"]["fmt_postedDate"]
    # # date_object = datetime.strptime(result["date"], "%b %d, %Y")
    # # result["year"] = date_object.year
    # # result["month"] = date_object.month
    # # result["day"] = date_object.day

        
    # if verbose: print "\tTitle: %s, Company: %s" % \
    #                      (result["title"],result["company"])
    print json.dumps(result, indent=2)
    return result

def removeTrailing(mydict):
    """ remove the trailing space in the value of a dictionary """
    for key in mydict: 
        value = mydict[key]
        if type(value) == str:
            mydict[key] = value.strip()

if __name__ == "__main__": main()


