# -*- coding: utf-8 -*-
import scrapy


class TestingSpider(scrapy.Spider):
    name = 'testing'
    allowed_domains = ['id.wikipedia.org']
    start_urls = ['https://id.wikipedia.org/wiki/Carrefour']

    def parse(self, response):
        #print(response.css('div.mw-parser-output').xpath('//li/a/@href').getall())
        #for link in response.css('div.mw-parser-output li > a::attr(href)').getall() :
        #    if 'index.php' not in link :
        #        if ':' not in link.encode('utf-8') :
        #            print(response.urljoin(link.encode('utf-8')))
        pass