#! python2
# -*- coding: utf-8 -*-
import scrapy
import json
import time
import csv
import os
import re
from scrapy import signals
from . import bumntodbmy
#import psycopg2
#from configdb import config

class BumnbotSpider(scrapy.Spider):
    name = 'bumnbot'
    allowed_domains = ['bumn.go.id']
    start_urls = ['http://bumn.go.id/halaman/jsondatasitus']
    timestr = time.strftime("%Y%m%d-%H%M%S")
    fieldnames = ['nomor','bumn', 'logo', 'sektor','situs']
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename1 = dir_path + '/../out/bumn_%s.csv' % timestr

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BumnbotSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider):
        spider.logger.info('Signal sent then Spider closed. file out is : %s', self.filename1)
        #self.connect()
        #bumntodb.readcsvandupdate(self.allowed_domains[0],self.filename1)
        bumntodbmy.readcsvandupdate(self.allowed_domains[0],self.filename1)
        # saving to mysql should load here

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        data = jsonresponse['aaData']
        with open(self.filename1, 'a') as f: 
            w = csv.writer(f, lineterminator='\n', delimiter='|',quotechar="'")
            w.writerow(self.fieldnames)
            for row in data:
                test1 = re.sub(r"<a.*?>", '', row[1])
                test2 = re.sub(r'</a>', '', test1)
                w.writerow([row[0],test2.strip(),row[2].strip(),row[3].strip(),row[4].strip()])