## Summary

linkedin module is used to scrape information from job listing boards and company profile page.

## Functions

### getCompanyInfo(companyid, verbose=False)

Linkedin assign an unique id for company who open public page on its website. For example, the following webpage is BlackStone Group's public page: https://www.linkedin.com/company/7834, and its company id is 7834. 

getCompanyInfo takes companyid as argument and return the company basic information, including type, founed, specialties, in dictionary. verbose can be set to True if the user want to print debuging information. 

### getJobInfo(jobid, verbose=False)

Linkedin assign an unique id for every available job post. For example, the following webpage is one job post on Linkedin https://www.linkedin.com/jobs2/view/11004170 and the corresponding jobid is 11004170

getJobInfo takes jobid as argument and return the job basic information, including job title, company, posted date and job descrption, in dictionary. verbose have the same functions as the getCompanyInfo function. 