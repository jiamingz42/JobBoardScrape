#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os 

def main():
    commnds = [ 'CURL "http://127.0.0.1:5984/company/_design/common/_view/allcom" > company.json',
                'CURL "http://127.0.0.1:5984/analytics2/_design/common/_view/all" > job.json',
                'CURL "http://127.0.0.1:5984/mapping/_design/common/_view/locationMapping" > location.json']

    for command in commnds: os.system(command)

if __name__ == "__main__": main()


