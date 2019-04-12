# -*- coding: utf-8 -*-
import scrapy
import time
from selenium import webdriver



class ForlapbotSpider(scrapy.Spider):
    name = 'forlapbot'
    allowed_domains = ['forlap.ristekdikti.go.id']
    start_urls = ['https://forlap.ristekdikti.go.id/perguruantinggi/']
    #handle_httpstatus_list = [302]

    def __init__(self):
        self.driver = webdriver.Chrome(executable_path='D:/scrapy_script/chromedriver_win32/chromedriver.exe')

    def parse(self, response):
        self.driver.get(response.url)
        time.sleep(2.5)
        #res = response.replace(body=self.driver.page_source)
        #yield {'body':response.css('body').extract()}
        #hit1 = res.xpath('//input[@name = "captcha_value_1"]').attrib['value']
        hit1 = self.driver.find_element_by_name('captcha_value_1').get_attribute("value")
        #hit2 = res.xpath('//input[@name = "captcha_value_2"]').attrib['value']
        hit2 = self.driver.find_element_by_name('captcha_value_2').get_attribute("value")
        #button = response.xpath('//input[@type=button]')[1]
        hit3 = int(hit1) + int(hit2)
        kode_pengaman = self.driver.find_element_by_name('kode_pengaman')
        kode_pengaman.send_keys(str(hit3))
        mybtn = self.driver.find_element_by_xpath('//input[@type="button"]')
        mybtn.click()
        time.sleep(10)
        #yield {'test':str(hit3)}
        #rows = self.driver.find_elements_by_class_name("ttop")
        #for row in rows:
        #    data = row.find_elements_by_tag_name('td')
        #    yield {
        #     'kode': data[1].text,
        #     'nama': data[2].text
        #    }
        #myactive = self.driver.find_elements_by_css_selector('li.active > span')
        #myactive = self.driver.find_element_by_xpath('//li[@class="active"]/following-sibling::li/a')
        #myli = myul.find_elements_by_tag_name('li')
        #yield {'test':myactive.get_attribute("href")}
        while True:
            mylia = self.driver.find_element_by_xpath('//li[@class="active"]/following-sibling::li/a')
            try:
                mylia.click()
                time.sleep(10)
                # get the data and write it to scrapy items
                rows2 = self.driver.find_elements_by_class_name("ttop")
                for row1 in rows2:
                    data1 = row1.find_elements_by_tag_name('td')
                    yield {
                     'kode': data1[1].text,
                     'nama': data1[2].text
                    }
            except:
                break
        time.sleep(2)
        self.driver.close()
        #yield {'test':plans}
        #self.driver.find_element_by_xpath('//input[@name = "kode_pengaman"]').attrib['value'] = str(hit3)
        #inputbtn = self.driver.find_element_by_xpath('//input[@class = "btn btn-primary"]')
        #inputbtn.click()
        #time.sleep(10)
        #return scrapy.http.FormRequest.from_response(response,
        #    url="https://forlap.ristekdikti.go.id/perguruantinggi/search",
        #    formnumber=1,
        #    formdata={'kode_pengaman':str(hit3),'searchfullpt':'','captcha_value_1':hit1,'captcha_value_2':hit2,'keyword':'','id_wil':'','kode_koordinasi':'','id_bp':'','stat_sp':''},
        #    callback=self.after_login
        #)
        #yield {'angka1':hit1, 'angka2':hit2 , 'angka3':hit3}
        #for alltr in  response.css('tr.ttop').getall():
        #    yield {
        #     'kode': alltr.css('td[1]::text').extract(),
        #     'nama': alltr.css('td[2]::text').extract()
        #     }
    def after_login(self, response):
        yield {'body':response.css('html').extract()}