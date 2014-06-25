#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
from datetime import datetime
import mechanize, json, time, heapq, re, couchdb

############################# 
## CONFIGURATION PARAMETER ##
#############################

# Linkedin Log in Parameter
KEYWORDS = ["Data Analyst CA"]
LOCATIONS = ["California","CA"]
USERNAME = "student23@163.com" 
PASSWORD = "jz4kFZRBi4"

# Linkedin Job Post Scraping Reuslt
# Source URL will be used as the ID
# Title, Company, Address, Keyword, Posting Time, Job Description

# CouchDB Paramter
DBADDRESS = 'http://127.0.0.1:5984'
DBNAME = "test" #"analytics"

# Time Interval
JOBINTERVAL = 0.2
PAGEINTERVAL = 2


def main():
    
    # connect the server
    couch = couchdb.Server(DBADDRESS)

    # check whether the db exists
    isExist = False
    for database in couch: 
        if database == DBNAME: isExist = True

    # connect the database or create it if not exist
    if isExist == True:
        database = couch[DBNAME]
    else:
        couch.create(DBNAME)
        database = couch[DBNAME]

    # intial the browser object
browser = mechanize.Browser()
browser.set_handle_robots( False )
browser.addheaders = [('User-agent', 'Firefox')]


# log in the linkedin
browser.open("https://www.linkedin.com/")
print "Open the Linkedin Login Page (Title: %s)" % browser.title()

browser.select_form(nr = 0)
browser.form['session_key'] = USERNAME 
browser.form['session_password'] = PASSWORD
browser.submit()
print "Entered Username/Password (Title: %s)" % browser.title()

    # @Todo: Include diff website if possible
    # @Todo: Handle Exception
    # @Todo: How to get as much as possible given the Linkedin first 1000 result constraint
    # @Todo: Set up a CouchDB in AWS
    for kw in KEYWORDS:
        # for loc in LOCATIONS:
            # search_term = "%s %s" % (kw,loc)
            # print "search_term", search_term
            # function does not return value
        searchJobInLinkedin(browser, database, kw)

    browser.close()
    print "Script Reach its End"




def searchJobInLinkedin(browser, database, keyword):

    base = "https://www.linkedin.com"
    browser.open(base)
    browser.select_form(nr = 0)
    browser.form['keywords'] = keyword
    resp = browser.submit()
    html_content = resp.get_data()
    print "Searched Keyword \"%s\" (Title: %s)" % (keyword, browser.title())
    

    url = "https://www.linkedin.com/vsearch/j?type=jobs&keywords=Data+Analyst+CA&orig=GLHD&rsid=1814069061403653896615&pageKey=voltron_federated_search_internal_jsp&trkInfo=&search=Search"
    https://www.linkedin.com/vsearch/j?type=jobs&keywords=Data+Analyst+CA
    resp = browser.open(url)
    html_content = resp.get_data()

    # inital the queue for page
    # soup = BeautifulSoup(html_content)
    # print soup 
    # print "***"
    pages = getInfo(html_content,"page")
    

    pageQueue, addedPage = [], set()
    for page in pages: 
        heapq.heappush(pageQueue, (page["pageNum"],page["pageURL"]))
        addedPage.add(page["pageNum"])

    while pageQueue:
        pageNum, pageURL = heapq.heappop(pageQueue)
        
        # get to the new search page
        resp = browser.open(base + pageURL) # pageURL
        html_content = resp.get_data()
        print "\n#### Current PageNum is %d ####" % pageNum

        # get the page info from this page and update pageQueue/addedPage
        navPages = getInfo(html_content,"page")
        lastPage = navPages[-1]
        if lastPage["pageNum"] not in addedPage:
            addedPage.add(lastPage["pageNum"])
            heapq.heappush(pageQueue, (lastPage["pageNum"], lastPage["pageURL"]))

        # get the job info
        jobs = getInfo(html_content,"job")
        
        for job in jobs:
            if job.get("job") != None:
                
                url = base + job['job']['actions']['link_viewJob_2']
                
                pattern = re.compile("jobId=([0-9]+)&")
                jobid = pattern.findall(url)[0]                
                
                if len(database.query(map_fun("Linkedin", jobid))) == 0:
                    result = dict()
                    result["jobid"] = jobid
                    result["url"] = url
                    result["source"] = "Linkedin" 
                    result["keyword"] = keyword

                    result["date"] = job["job"]["fmt_postedDate"]
                    date_object = datetime.strptime(result["date"], "%b %d, %Y")
                    result["year"] = date_object.year
                    result["month"] = date_object.month
                    result["day"] = date_object.day

                    getJobData(browser, url, result)

                    # remove the trailing space of each field if value is string
                    removeTrailing(result)
                    
                    # save the job info to couchdb
                    database.save(result)

                    print "Title: %s\nCompany: %s\n--------------" % \
                            (result["title"],result["company"])
                else:
                    print "Found Existed Record in the Database"

                time.sleep(JOBINTERVAL)

        time.sleep(PAGEINTERVAL)
        break

def getInfo(html_content,infotype):
    soup = BeautifulSoup(html_content)

    # fix the weird bug of the retrieved json
    pattern = re.compile(":\\\\u002d1")
    try:
        code = pattern.sub(lambda match: ':"%s"' % match.group(), soup.code.string)
    except TypeError:
        print "infotype=%s" % infotype 
        print type(soup.code)
        print soup.code
        raise TypeError

    # extract the job/page info based on the keychain
    keychain = {"page":["content","page","voltron_unified_search_json",
                        "search","baseData","resultPagination","pages"],
                "job": ["content", "page", "voltron_unified_search_json", 
                        "search", "results"]}
    code = json.loads(code)
    result = getDictValue(code, keychain[infotype])
    
    return result

def getDictValue(mydict, keychain):
    """ return the value in a nested dictionary using a order-list 
    where the former element represent a higher level key """
    res = mydict
    for key in keychain:
        if res.get(key) != None:
            res = res[key]        
        else:
            return None
    return res

def getJobData(browser, url, result):
    """ Funtion returns structured data from indivudal Linkedin job listing page """
    
    resp = browser.open(url)
    html_content = resp.get_data()
    soup = BeautifulSoup(html_content)
    browser.back()

    # job title and company name
    result["title"] = soup.find("h1", class_="title").string
    result["company"] = soup.find("h2", class_="sub-header").span.string

    # job location related filed
    jobLocation = soup.find("h2", class_="sub-header").find("span", itemprop="jobLocation")
    result["location"] = jobLocation.find("span", itemprop="description").string
    result["address"] = jobLocation.find("meta", itemprop="addressLocality").attrs["content"]
    result["region"] = jobLocation.find("meta", itemprop="addressRegion").attrs["content"]
    result["country"] = jobLocation.find("meta", itemprop="addressCountry").attrs["content"]
    
    # jd keeps the markdown structure                   
    result["jd"] = str(soup.find("div", class_="description-module container"))

def map_fun(source, jobid):
    """ wrapper for map function used by the couchdb to search whether an record
    with a specific url exists """
    
    return """ function(doc) {
                if (doc.source = "%s" && doc.jobid == "%s") {
                    emit(doc.title, null);
                }
              } """ % (source, jobid)

def removeTrailing(mydict):
    """ remove the trailing space in the value of a dictionary """
    for key in mydict: 
        value = mydict[key]
        if type(value) == str:
            mydict[key] = value.strip()

def searchLink(keyword, pageNum=1, sort="R", zipcode=None, radius=None):
    # Linkedin Job Search Link Structure (searchLinkConstruct)
    # base: https://www.linkedin.com/vsearch/j?type=jobs
    # keyword: &keywords=Data+Analyst+CA
    # sorted: (earliest) &sortBy=DA  (recent) &sortBy=DD  (relevant) &sortBy=R
    # zipcode and radius: &postalCode=94085&distance=50
    # country: countryCode=us
    # page: &page_num=2

    # Linkedin will return all active job post based on this link
    url = "https://www.linkedin.com/vsearch/j?type=jobs" 

    url += "&keywords=%s" % keyword.replace(" ","+") # include keyword (required)
    url += "&sortBy=%s" % sort # include sorted 

    url += "&countryCode=us" # only search us
    url += ("&postalCode=%d" % zipcode) if zipcode != None else "" 
    url += ("&distance=%d" % radius) if (None not in (zipcode,radius)) else ""

    url += "&page_num=%d" % pageNum

    return url



if __name__ == "__main__": main()




