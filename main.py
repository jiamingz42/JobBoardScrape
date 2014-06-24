#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import mechanize, json, time, heapq, re, couchdb

############################# 
## CONFIGURATION PARAMETER ##
#############################

# Linkedin Log in Parameter
KEYWORDS = ["Data Analyst"]
USERNAME = "student23@163.com" 
PASSWORD = "jz4kFZRBi4"

# CouchDB Paramter
DBADDRESS = 'http://127.0.0.1:5984'
DBNAME = "analytics"

# Time Interval
JOBINTERVAL = 0.2
PAGEINTERVAL = 2


def main():

    # connect the server
    couch = couchdb.Server(DBADDRESS)

    # connect the database
    db = couch[DBNAME]

    # @Todo: Handle Exception
    # @Todo: How to get as much as possible given the Linkedin first 1000 result constraint
    # @Todo: Check duplicate result in CouchDB Database
    # @Todo: Set up a CouchDB in AWS
    # @Todo: include source, link, keyword, time
    for keyword in KEYWORDS:
        try:
            # function does not return value
            getJobDataByKeyword(keyword, db)
        except e:
            print e


def getJobDataByKeyword(keyword,database):

    browser = mechanize.Browser()

    browser.set_handle_robots( False )
    browser.addheaders = [('User-agent', 'Firefox')]

    browser.open("https://www.linkedin.com/")
    print "Open the Linkedin Login Page (Title: %s)" % browser.title()

    browser.select_form(nr = 0)
    browser.form['session_key'] = USERNAME 
    browser.form['session_password'] = PASSWORD
    browser.submit()
    print "Entered Username/Password (Title: %s)" % browser.title()

    browser.select_form(nr = 0)
    # multiple key words?
    browser.form['keywords'] = keyword
    resp = browser.submit()
    html_content = resp.get_data()
    print "Searched Keyword \"%s\" (Title: %s)" % (keyword, browser.title())

    # inital the queue for page
    pages = getInfo(html_content,"page")
    pageQueue, addedPage = [], set()
    for page in pages: 
        heapq.heappush(pageQueue, (page["pageNum"],page["pageURL"]))
        addedPage.add(page["pageNum"])

    base = "https://www.linkedin.com"
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
                url = job['job']['actions']['link_viewJob_2']
                res = getJobData(browser, base + url)
                res["keyword"] = keyword
                print "Title: %s\nCompany: %s\n--------------" % (res["title"],res["company"])
                database.save(res)
                
                time.sleep(JOBINTERVAL)

        time.sleep(PAGEINTERVAL)
        break


    browser.close()
        

  




def getInfo(html_content,infotype):
    soup = BeautifulSoup(html_content)

    # fix the weird bug of the retrieved json
    pattern = re.compile(":\\\\u002d1")
    code = pattern.sub(lambda match: ':"%s"' % match.group(), soup.code.string)

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

def getJobData(browser, url):
    """ return structured data from linkedin job description web page """
    result = dict()

    resp = browser.open(url)
    html_content = resp.get_data()
    soup = BeautifulSoup(html_content)
    browser.back()

    result["title"] = soup.find("h1", class_="title").string
    
    result["company"] = soup.find("h2", class_="sub-header").span.string

    jobLocation = soup.find("h2", class_="sub-header").find("span", itemprop="jobLocation")
    result["location"] = jobLocation.find("span", itemprop="description").string
    result["address"] = jobLocation.find("meta", itemprop="addressLocality").attrs["content"]
    result["region"] = jobLocation.find("meta", itemprop="addressRegion").attrs["content"]
    result["country"] = jobLocation.find("meta", itemprop="addressCountry").attrs["content"]

    # add time
    
    # jd keeps the markdown structure                   
    result["jd"] = str(soup.find("div", class_="description-module container"))

    for key in result: result[key] = result[key].strip()

    return result


if __name__ == "__main__": main()




