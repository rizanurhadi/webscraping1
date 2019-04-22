# -*- coding: utf-8 -*-
import scrapy


class WikimallbotSpider(scrapy.Spider):
    name = 'wikimallbot'
    allowed_domains = ['id.wikipedia.org/wiki/Daftar_pusat_perbelanjaan_di_Indonesia']
    start_urls = ['http://id.wikipedia.org/wiki/Daftar_pusat_perbelanjaan_di_Indonesia/']

    def parse(self, response):
        pass
