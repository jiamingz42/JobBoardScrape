## Summary

I wrote this script to practise my web scraping skills, and to conduct an analysis about the characteristics of the demand of analytical positions. 

The package contains several modules/scripts:
- linkedin: linkedin module is used to scrape information from job listing boards and company profile page.
- location: map the raw location to standard location format using the Google Geocoded service from Python package GeoPy

## Dependency

- GeoPy
  - Documentation: http://geopy.readthedocs.org/en/latest/index.html#

## Linkedin Package

### getCompanyInfo(companyid, verbose=False)

Linkedin assign an unique id for company who open public page on its website. For example, the following webpage is BlackStone Group's public page: https://www.linkedin.com/company/7834, and its company id is 7834. 

**getCompanyInfo** takes companyid as argument and return the company basic information, including type, founed, specialties, in dictionary. verbose can be set to True if the user want to print debuging information. 

### getJobInfo(jobid, verbose=False)

Linkedin assign an unique id for every available job post. For example, the following webpage is one job post on Linkedin https://www.linkedin.com/jobs2/view/11004170 and the corresponding jobid is 11004170

**getJobInfo** takes jobid as argument and return the job basic information, including job title, company, posted date and job descrption, in dictionary. verbose have the same functions as the getCompanyInfo function. 

### searchJobLink(keyword, pageNum=1, sort="R", zipcode=None, radius=None)

**searchLink** generate the query url for search a job based on the given keyword and other argument. Here is an example generated link: https://www.linkedin.com/vsearch/j?type=jobs&keywords=data+analyst&sortBy=R&countryCode=us&page_num=1

Every job search link starts with "https://www.linkedin.com/vsearch/j?type=jobs". When you type this link in the browser, it return all available job post in Linkedin. The symbol "&" is used to concatenated different criteria. 
- "keywords=" represents the keyword. 
- "sortBy=" represents "sort by" and can take three values - R for revelent, DA for earliest and DD for most recent posts. 
- "zipcode=" can be used to specify the searched regions
- "radius=" can be used to specify the radius of the searched region when zipcode is specified
- "pageNum=" can be used to the specify the page num of the search result. Note that Linkedin only make the first 40 pages avaialble to normal user. 
 
## location

Location information scraped from the source website are usually not in a standard format. For example, "Chicago, IL" or "The Greater Chicago Area" refer to the same area but are typed in different format. The purpose of the location.py is to standardize the location into (State, County, City) format using Google geocoded service. 
