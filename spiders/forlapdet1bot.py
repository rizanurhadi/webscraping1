# -*- coding: utf-8 -*-
import scrapy


class Forlapdet1botSpider(scrapy.Spider):
    name = 'forlapdet1bot'
    allowed_domains = ['forlap.ristekdikti.go.id/perguruantinggi/detail/']
    start_urls = ['http://forlap.ristekdikti.go.id/perguruantinggi/detail//']

    def parse(self, response):
        pass
