#! python2
# -*- coding: utf-8 -*-
import scrapy
import json
import time
import csv
import re

class BumnbotSpider(scrapy.Spider):
    name = 'bumnbot'
    allowed_domains = ['bumn.go.id/halaman/jsondatasitus']
    start_urls = ['http://bumn.go.id/halaman/jsondatasitus']

    def parse(self, response):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        jsonresponse = json.loads(response.body_as_unicode())
        data = jsonresponse['aaData']
        self.log('data ' + str(len(data)))
        #self.log(jsonresponse)
        with open('bumn_%s.json' % timestr, 'w') as f: 
            f.write(json.dumps(jsonresponse))

        fieldnames = ['nomor','bumn', 'logo', 'sektor','situs']
        with open('bumn_%s.csv' % timestr, 'a') as f: 
            w = csv.writer(f, lineterminator='\n', delimiter='|',quotechar="'")
            w.writerow(fieldnames)
            for row in data:
                test1 = re.sub(r"<a.*?>", '', row[1])
                test2 = re.sub(r'</a>', '', test1)
                w.writerow([row[0],test2,row[2],row[3],row[4]])

