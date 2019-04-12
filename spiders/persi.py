# -*- coding: utf-8 -*-
import scrapy
from sys import exit


class PersiSpider(scrapy.Spider):
    name = 'persi'
    allowed_domains = ['persi.or.id']
    start_urls = ['http://www.persi.or.id/']

    def parse(self, response):
        for href in response.xpath('//a/@href').getall():
            #yield {"title": href}
            if href is not None and len(href) > 1 and ("http" not in href and "#" not in href and ".pdf" not in href and ".jpg" not in href):
                yield response.follow(href, self.parse_article)
            #exit(0)
    
    def parse_article(self, response):
        yield {
            'url': response.url,
            'title': response.css('article > div > h1::text').extract(),
            'body': response.css('article > div > div').extract(),
        }