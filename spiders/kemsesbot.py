# -*- coding: utf-8 -*-
import scrapy


class KemsesbotSpider(scrapy.Spider):
    name = 'kemsesbot'
    allowed_domains = ['sirs.yankes.kemkes.go.id/rsonline/DATA_RUMAH_SAKIT_REPORT_report.php?pagesize=-1']
    start_urls = ['http://sirs.yankes.kemkes.go.id/rsonline/DATA_RUMAH_SAKIT_REPORT_report.php?pagesize=-1/']

    def parse(self, response):
        pass
