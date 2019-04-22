# -*- coding: utf-8 -*-
import scrapy


class KemendagbotSpider(scrapy.Spider):
    name = 'kemendagbot'
    allowed_domains = ['www.kemendag.go.id/id/perdagangan-kita/company-directory/data-center-collection/?N=1']
    start_urls = ['http://www.kemendag.go.id/id/perdagangan-kita/company-directory/data-center-collection/?N=1/']

    def parse(self, response):
        pass
