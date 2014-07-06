#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pandas as pd
from pandas import Series, DataFrame, ExcelWriter

def main():
    df_job = getDataFrame("job.json",["jobid","companyid"])
    df_company = getDataFrame("company.json", ["companyid","Name",'Founded','Industry','Type','Size'])
    df_location = getDataFrame("location.json",["city", "county", "state","country"])
    
    with ExcelWriter('output.xlsx') as writer:
        df_job.to_excel(writer, sheet_name='job')
        df_company.to_excel(writer, sheet_name='company')
        df_location.to_excel(writer, sheet_name='location')

def getDataFrame(infile, index):
    raw = getJson(infile)
    records = raw["rows"]
    result = []
    for record in records:
        if record["value"] == None: continue
        for (i,key) in enumerate(index): 
            if i == 0: value = []
            value.append(record["value"].get(key))
        series = Series(value,index=index)
        result.append(series)
    df = DataFrame(result,columns=index)
    return df
    

    # df.to_excel('path_to_file.xlsx', sheet_name='Sheet2')

def getJson(infile):
    fh = open(infile,"r")
    return json.loads(fh.read())

if __name__ == "__main__": main()