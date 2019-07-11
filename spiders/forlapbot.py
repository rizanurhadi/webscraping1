# -*- coding: utf-8 -*-
#! python2
import scrapy
import time
import csv
from selenium import webdriver
#from scrapy.selector import Selector
import os
from scrapy import signals
import forlapbottodbmy


class ForlapbotSpider(scrapy.Spider):
    name = 'forlapbot'
    allowed_domains = ['forlap.ristekdikti.go.id']
    start_urls = ['https://forlap.ristekdikti.go.id/perguruantinggi/']
    fieldnames = ['kode','nama', 'link_detail', 'prov','kategori','status','jml_dosen_tetap_1718', 'jml_mhs_1718' , 'rasio_dosen_mhs_1718', 'jml_dosen_tetap_1819', 'jml_mhs_1819' , 'rasio_dosen_mhs_1819']
    dir_path = os.path.dirname(os.path.realpath(__file__))
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename1 = dir_path + '/../out/forlap_pt_header_%s.csv' % timestr
    filename2 = dir_path + '/../out/forlap_perguruan_tinggi.csv'
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ForlapbotSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider):
        spider.logger.info('Signal sent then Spider closed. file out is : %s', self.filename1)
        #save to db here
        forlapbottodbmy.readcsvandupdate(self.allowed_domains[0],self.filename1)


    def __init__(self):
        self.driver = webdriver.Chrome(executable_path='D:/scrapy_script/chromedriver_win32/chromedriver.exe')

    def parse(self, response):
        self.driver.get(response.url)
        time.sleep(2.5)
        hit1 = self.driver.find_element_by_name('captcha_value_1').get_attribute("value")
        hit2 = self.driver.find_element_by_name('captcha_value_2').get_attribute("value")
        hit3 = int(hit1) + int(hit2)
        kode_pengaman = self.driver.find_element_by_name('kode_pengaman')
        kode_pengaman.send_keys(str(hit3))
        mybtn = self.driver.find_element_by_xpath('//input[@type="button"]')
        mybtn.click()
        time.sleep(10)
        iterasi = 1
        with open(self.filename1, 'a') as f:
            rows = self.driver.find_elements_by_class_name("ttop")
            #item = BasicItem()
            for row in rows:
                iterasi += 1
                data = row.find_elements_by_tag_name('td')
                link_detail = row.find_element_by_tag_name('a')
                myyield = self.make_yeld(data,link_detail)  
                w = csv.DictWriter(f, self.fieldnames, lineterminator='\n', delimiter='|') 
                if iterasi ==2 : 
                    w.writeheader()
                w.writerow(myyield)
                self.write_secondfile(myyield,iterasi)
        
            gotroughpage = False
            if gotroughpage == True :
                while True:
                    mylia = self.driver.find_element_by_xpath('//li[@class="active"]/following-sibling::li/a')
                    try:
                        mylia.click()
                        time.sleep(2.5)
                        # get the data and write it to scrapy items
                        rows2 = self.driver.find_elements_by_class_name("ttop")
                        for row1 in rows2:
                            data1 = row1.find_elements_by_tag_name('td')
                            link_detail2 = row1.find_element_by_tag_name('a')
                            #yield self.make_yeld(data1,link_detail2)
                            myyield = self.make_yeld(data1,link_detail2)  
                            w = csv.DictWriter(f, self.fieldnames, lineterminator='\n', delimiter='|') 
                            #if iterasi ==2 : 
                            #    w.writeheader()
                            w.writerow(myyield)
                            self.write_secondfile(myyield,iterasi)
                    except:
                        break
            time.sleep(2)
            self.driver.quit()
    
    def make_yeld(self, data,link_detail):
        return {
             'kode': data[1].text.strip(),
             'nama': data[2].text.strip(),
             'link_detail' : link_detail.get_attribute("href").strip(),
             'prov': data[3].text.strip(),
             'kategori': data[4].text.strip(),
             'status': data[5].text.strip(),
             'jml_dosen_tetap_1718': data[6].text.strip(),
             'jml_mhs_1718': data[7].text.strip(),
             'rasio_dosen_mhs_1718': data[8].text.strip(),
             'jml_dosen_tetap_1819': data[9].text.strip(),
             'jml_mhs_1819': data[10].text.strip(),
             "rasio_dosen_mhs_1819": data[11].text.strip()
            }
    def parse_detail(self, response) :
        mytable = response.xpath('//table[@class="table table-bordered"]') 
        i = 0
        for alltr in mytable[1].css('tr') : 
            i += 1
            if i > 2 :
                yield { 
                    'kodept':response.meta.get('kodept'),
                    'kode' : alltr.xpath('td[2]//text()').get() ,
                    'nama' : alltr.xpath('td[3]//text()').get() 
                    }
            #yield { 'nama': alltr.css('td[2]::text').extract() }
        #for alltr in  mytable.css('tr') :
        #    yield { 
        #        'kode': alltr.css('td[1]::text').extract(), 
        #        'nama': alltr.css('td[2]::text').extract()
        #         }
    def parse_page(self,response):
        for row in response.css('tr.ttop'):
          data = row.css('td')
          kodept =data[1].css('::text').get()
          yield response.follow(data[2].css('a::attr(href)').get(), self.parse_detail, meta={'kodept': kodept})

    def write_secondfile(self,myyield,iterasi):
        if iterasi ==2 :
            with open(self.filename2, 'w') as f:
                w = csv.DictWriter(f, ['kode','nama','link_detail'], lineterminator='\n', delimiter='|') 
                if iterasi ==2 : 
                    w.writeheader()
                w.writerow({'kode':myyield['kode'],'nama':myyield['nama'],'link_detail':myyield['link_detail']})
        else :
            with open(self.filename2, 'a') as f:
                w = csv.DictWriter(f, ['kode','nama','link_detail'], lineterminator='\n', delimiter='|') 
                w.writerow({'kode':myyield['kode'],'nama':myyield['nama'],'link_detail':myyield['link_detail']})