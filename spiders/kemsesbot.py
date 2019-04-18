# -*- coding: utf-8 -*-
import scrapy
import time
from selenium import webdriver



class KemsesbotSpider(scrapy.Spider):
    name = 'kemsesbot'
    allowed_domains = ['sirs.yankes.kemkes.go.id']
    start_urls = ['http://sirs.yankes.kemkes.go.id/rsonline/DATA_RUMAH_SAKIT_REPORT_report.php?pagesize=-1/']

    def __init__(self):
        self.driver = webdriver.Chrome(executable_path='D:/scrapy_script/chromedriver_win32/chromedriver.exe')
    
    def parse(self, response):
        self.driver.get(response.url)
        time.sleep(2.5)
        rows = self.driver.find_elements_by_xpath('//tr')
        #yield { 'table' : rows.get() }
        i = 0
        for row in rows :
            i += 1
            if i > 6 :
                data = row.find_elements_by_tag_name('td')
                yield {
                    'kode' : data[0].text,
                    'tgl_registrasi':data[1].text,
                    'nama':data[2].text,
                    'jenis':data[3].text,
                    'kelas':data[4].text,
                    'direktur':data[5].text,
                    'alamat':data[6].text,
                    'penyelenggara':data[7].text,
                    'kab_kota':data[8].text,
                    'kodepos':data[9].text,
                    'telephone':data[10].text,
                    'fax':data[11].text,
                    'tgl_update':data[12].text,
                }
        time.sleep(2)
        self.driver.quit()
