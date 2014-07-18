#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
from geopy.geocoders import GoogleV3
import couchdb
import re, json, math, random, time, datetime
import mechanize, urllib2, json, bs4

############################# 
## CONFIGURATION PARAMETER ##
#############################

# CONFIG = json.loads(open("config.json","r").read())
# DBADDRESS = CONFIG["database"]["couchdb_address"]
# LOGDBNAME = CONFIG["database"]["log_dbname"]
# SCRAPEDOCID = CONFIG["current_scrape"]
# SCRAPEDOC = LOGDBNAME[SCRAPEDOCID]

def main():
    parseLocationRaw("rawLocation")


def config(configFile):
    config = open(configFile,"r")
    return json.loads(config.read())

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
        couchdb.create(dbname)
        database = couchdb[dbname]

    return database

############################# 
###### SCRAPE FUNCTION ######
#############################

def searchJobLink(keyword, pageNum=1, sort="R", zipcode=None, radius=None, exact=False):

    # Linkedin will return all active job post based on this link
    url = "https://www.linkedin.com/vsearch/j?type=jobs" 

    # keyword
    mappings = [("+","%2B"),(" ","+"),('"',"%22")]
    for (origin, new) in mappings:
        keyword = keyword.replace(origin,new)
    url += "&keywords=%s" % keyword
    
    url += "&sortBy=%s" % sort # include sorted 

    url += "&countryCode=us" # only search us
    url += ("&postalCode=%s" % zipcode) if zipcode != "00000" else "" 
    url += ("&distance=%d" % radius) if (zipcode != "00000" and radius != None) else ""

    url += "&page_num=%d" % pageNum

    return url

def getCompanyInfo(companyid, verbose=False):

    result = {"companyid"   : str(companyid),
              "companyURL"  : "https://www.linkedin.com/company/%s" % str(companyid)}

    html = _getHTML(result["companyURL"])
    soup = BeautifulSoup(html)

    # Wrapper Functions to Extract Certian Info from the Page
    getName    = lambda soup: soup.find("span", itemprop="name").text.strip()
    getInd     = lambda soup: soup.find("li", class_="industry").find("p").text.strip()
    getType    = lambda soup: soup.find("li", class_="type").find("p").text.strip()
    getSize    = lambda soup: soup.find("li", class_="company-size").find("p").text.strip()
    getFounded = lambda soup: soup.find("li", class_="founded").find("p").text.strip()
    getSp      = lambda soup: soup.find("div", class_="specialties").find("p").text.replace("\n","").split(",")
    
    funList = [("name"          ,getName    ),
               ("industry"      ,getInd     ),
               ("type"          ,getType    ),
               ("size"          ,getSize    ),
               ("founded"       ,getFounded ),
               ("specifities"   ,getSp      )]

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

    result = { "type"        : "job",
               "jobid"       : jobid,
               "source"      : "Linkedin",
               "scrapetime"  : str(datetime.datetime.now()),
               "jobURL"      : "https://www.linkedin.com/jobs2/view/%s" % jobid }

    html = urllib2.urlopen(result["jobURL"])
    soup = BeautifulSoup(html)

    # Wrapper Functions to Extract Certian Info from the Page
    def getName(soup):
        name = soup.find("h1", itemprop="title").text
        return name.strip()
    def getCompany(soup):
        search = soup.find("h2", class_="sub-header")
        company = search.span.text
        return company.strip()
    def getCompanyID(soup):
        search = soup.find("h2", class_="sub-header")
        if search.a == None:
            companyid = None
        else:
            company_link = search.a.attrs["href"]
            patterns = re.compile("/([0-9]+)")
            companyid = patterns.findall(company_link)[0]
        return unicode(companyid)
    def getLocation(soup):
        location = soup.find("span", itemprop="jobLocation").text
        return location.strip()
    def getDate(soup):
        deltaText = soup.find("div", class_="posted").text
        if "less than an hour" in deltaText:
            deltaDate = datetime.timedelta(hours=0)
        else:    
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
        return {"year": pos.year, "month": pos.month, "day": pos.day}
    def getJobd(soup):
        description = soup.find("div",class_="description-module container")
        newsoup = BeautifulSoup(unicode(description))
        jd = unicode("")
        for element in newsoup.body.next_elements:
            if (type(element) == bs4.element.NavigableString and
                element.strip() != ""):
                jd += unicode("\n") + element 
        return jd.strip()

    funList = [("title",        getName         ),
               ("company",      getCompany      ),
               ("companyid",    getCompanyID    ),
               ("location",     getLocation     ),
               ("date",         getDate         ),
               ("description",  getJobd         )]

    for (key, fun) in funList:
        try:
            result[key] = fun(soup)
        except Exception as e:
            print "JobID: %s; Key: %s" % (jobid, key)
            raise e

    message = "\tTitle: %s, Company: %s"    
    if verbose: print message % (repr(result["title"]),repr(result["company"]))
                         
    return result

def getJobCount(browser, keyword, zipcode, radius=20):

    # Wrapper Functions to Extract Certian Info from the Page
    def getResultCount(soup):
        code = json.loads(soup.code.string)
        keychain = ["content",
                    "page",
                    "voltron_unified_search_json",
                    "search",
                    "baseData",
                    "resultCount"]
        resultCount = _getDictValue(code, keychain)
        reusltPerPage = 25
        pageCount = int(math.ceil(float(resultCount)/reusltPerPage))        
        return resultCount, pageCount 
    
    # obtain the search URL/HTML
    argv = {"keyword" : keyword                        ,
            # "pageNum" : pageNum                       ,
            # "sort"    : "DD" if counter < 40 else "DA" ,
            "zipcode" : zipcode                        ,
            "radius"  : radius                         } 
    url = searchJobLink(**argv)
    html = browser.get_data(url)
    soup = BeautifulSoup(html)
    resultCount, pageCount = getResultCount(soup)
    
    return resultCount

def getJobIDByKeyword(browser, keyword, zipcode, radius=20, verbose=False):
    """ broswer is an mechanize.Browser instance that has logined Linkedin. """

    # Wrapper Functions to Extract Certian Info from the Page
    def getResultCount(soup):
        code = json.loads(soup.code.string)
        keychain = ["content",
                    "page",
                    "voltron_unified_search_json",
                    "search",
                    "baseData",
                    "resultCount"]
        resultCount = _getDictValue(code, keychain)
        reusltPerPage = 25
        pageCount = int(math.ceil(float(resultCount)/reusltPerPage))        
        return resultCount, pageCount 
    def getJobID(soup):
        result = []
        code = json.loads(soup.code.string)
        keychain = ["content",
                    "page",
                    "voltron_unified_search_json",
                    "search",
                    "results"]
        jobsInfo = _getDictValue(code, keychain)
        for jobInfo in jobsInfo:
            if jobInfo.get("job") == None: continue
            jobid = jobInfo["job"]["id"]
            result.append(jobid)
        return result
    
    if keyword not in browser.scrapedoc["queue"]:
        browser.scrapedoc["queue"][keyword] = dict()
    if zipcode not in browser.scrapedoc["queue"][keyword]: 
        browser.scrapedoc["queue"][keyword][zipcode] = dict()

    subdoc = browser.scrapedoc["queue"][keyword][zipcode]
    if "-1" in subdoc: return subdoc
    

    counter, maxPageNum = 0, 40
    while counter == 0 or counter < min(pageCount, maxPageNum*2):

        # obtain the search URL/HTML
        pageNum = counter % maxPageNum + 1
        argv = {"keyword" : keyword                        ,
                "pageNum" : pageNum                       ,
                "sort"    : "DD" if counter < 40 else "DA" ,
                "zipcode" : zipcode                        ,
                "radius"  : radius                         ,
                "exact"   : True                           } 
        url = searchJobLink(**argv)
        html = browser.getHTML(url)
        soup = BeautifulSoup(html)

        # only implement during the first round of the loop
        if counter == 0: 
            resultCount, pageCount = getResultCount(soup)
            if verbose: print "pageCount: %d >> Current Page: " % pageCount,
            # if the zipcode has never been searched 
            if len(subdoc) == 0 or "1" not in subdoc: 
                pass # do nothing
            # if the search for this zipcode has been completed
            elif int(max(subdoc,key=int)) == min(2*maxPageNum,pageCount): 
                browser.scrapedoc["queue"][keyword][zipcode]["-1"] = {}
                browser.updateLog()
                break
            # the given zipcode is partialy done
            else:
                counter = int(max(subdoc,key=int))
                continue
        
        time.sleep(5)
        
        subdoc[counter+1] = getJobID(soup)
        browser.logdb.save(browser.scrapedoc)
        
        if verbose: print "%d, " % pageNum,    
        counter += 1

    if verbose: print

    browser.scrapedoc["queue"][keyword][zipcode]["-1"] = {}
    browser.updateLog() 

    return subdoc


def _getDictValue(mydict, keychain):
    """ return the value in a nested dictionary using a order-list 
    where the former element represent a higher level key """
    res = mydict
    for key in keychain:
        if res.get(key) != None:
            res = res[key]        
        else:
            return None
    return res

def _getHTML(url, retry=5, verbose=True):
    # set up the http proxy server 
    if CONFIG["auto_proxy"] == True:
        proxyList = CONFIG["http_proxy"]
        http_proxy = random.choice(proxyList)
        proxy = urllib2.ProxyHandler({'http': http_proxy})
        opener = urllib2.build_opener(proxy)
        urllib2.install_opener(opener)
        if verbose: print http_proxy,
    
    # try multiple time
    for i in xrange(retry):
        try:
            print ":",
            resp = urllib2.urlopen(url)
            break
        except Exception:
            continue
    if verbose: print 
    return resp

############################# 
####### DATA CLEANING #######
#############################

def parseLocationRaw(rawLocation, retry=10):
    geolocator = GoogleV3(api_key="AIzaSyDEHdot09xx2PidK5-jAkz8GwmOYtNjbJQ",proxies={"http":"50.202.206.188:3128"})
    for i in xrange(retry):
        try:
            location = geolocator.geocode(rawLocation)
            break
        except:
            time.sleep(5)
            continue

    if location == None: return None

    # location.raw return a list of item
    # each of which represents a component of the address, including state, city
    # var mappings indicates this relationship

    mappings = [ { "mapping_key": "country",
                   "types": [u'country', u'political']},

                 { "mapping_key": "state",
                   "types": [u'administrative_area_level_1', u'political']},

                 { "mapping_key": "county",
                   "types": [u'administrative_area_level_2', u'political']},

                 { "mapping_key": "city",
                   "types": [u'locality', u'political']}
                 ]

    geocoded = dict()
    for item in location.raw["address_components"]:
        for mapping in mappings:
            mapping_key = mapping["mapping_key"]
            types = mapping["types"]
            if item["types"] == types: 
                geocoded[mapping_key] = item["long_name"]

    return geocoded

def viewToSet(view):
        result = set()
        for row in view:
            key = row.key
            result.add(key)
        return result 


############################# 
########## BROWSER ##########
#############################

class ExtendedBrowser(mechanize.Browser):
    def init(self):
        # attach the database to the browser object
        self.config = config("config.json")
        self.dbaddress = self.config["database"]["couchdb_address"]
        self.couchdb = couchdb.Server(self.dbaddress)
        self.jobdb_name = self.config["database"]["job_dbname"]
        self.jobdb = connectdb(self.couchdb, self.jobdb_name)
        self.logdb_name = self.config["database"]["log_dbname"]
        self.logdb = connectdb(self.couchdb, self.logdb_name)
        self.scrapedocid = self.config["current_scrape"]
        self.scrapedoc = self.logdb[self.scrapedocid]

        # set up the anti-robots detect
        self.set_handle_robots( False )
        self.addheaders = [('User-agent', 'Firefox')]

        # set up a random http proxy
        # choice = random.choice(self.config["http_proxy"])
        # self.set_proxies({"http": choice})


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

    def getHTML(self, url,goback=False):
        resp = self.open(url)
        html = resp.get_data()
        return html

    def updateLog(self):
        self.logdb.save(self.scrapedoc)

if __name__ == "__main__": main()


