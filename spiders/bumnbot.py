#! python2
# -*- coding: utf-8 -*-
import scrapy
import json
import time
import csv
import os
import re

class BumnbotSpider(scrapy.Spider):
    name = 'bumnbot'
    allowed_domains = ['bumn.go.id/halaman/jsondatasitus']
    start_urls = ['http://bumn.go.id/halaman/jsondatasitus']
    timestr = time.strftime("%Y%m%d-%H%M%S")
    fieldnames = ['nomor','bumn', 'logo', 'sektor','situs']
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename1 = dir_path + '/../out/bumn_%s.csv' % timestr

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        data = jsonresponse['aaData']
        with open(self.filename1, 'a') as f: 
            w = csv.writer(f, lineterminator='\n', delimiter='|',quotechar="'")
            w.writerow(self.fieldnames)
            for row in data:
                test1 = re.sub(r"<a.*?>", '', row[1])
                test2 = re.sub(r'</a>', '', test1)
                w.writerow([row[0],test2,row[2],row[3],row[4]])