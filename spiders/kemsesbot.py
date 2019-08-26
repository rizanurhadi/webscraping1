#! python2
# -*- coding: utf-8 -*-
import scrapy
import time
import csv
import os 
from selenium import webdriver
import logging 
from selenium.webdriver.chrome.options import Options
#from scrapy.utils.log import configure_logging  
from scrapy import signals
from . import kemkesbottodbmy

class KemsesbotSpider(scrapy.Spider):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    #configure_logging(install_root_handler = False) 
    #logging.basicConfig ( 
    #    filename = dir_path + '/../out/log_kemkes.txt', 
    #    format = '%(levelname)s: %(message)s', 
    #    level = logging.INFO 
    #)
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_argument('--disable-dev-shm-usage')
    timestr = time.strftime("%Y%m%d-%H%M%S")
    name = 'kemsesbot'
    allowed_domains = ['sirs.yankes.kemkes.go.id']
    start_urls = ['http://sirs.yankes.kemkes.go.id/rsonline/DATA_RUMAH_SAKIT_REPORT_report.php?pagesize=-1/']
    fieldnames = ['kode','nama','tgl_registrasi','jenis','kelas','direktur','alamat','penyelenggara','kab_kota','kodepos','telephone','fax','tgl_update','produk']
    filename1 = dir_path + '/../out/kemkesbot_%s.csv' % timestr

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KemsesbotSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider):
        spider.logger.info('Signal sent then Spider closed. file out is : %s', self.filename1)
        #save to db here
        kemkesbottodbmy.readcsvandupdate(self.allowed_domains[0],self.filename1)
    
    def __init__(self):
        self.driver = webdriver.Chrome(executable_path='D:/scrapy_script/chromedriver_win32/chromedriver.exe')
        #self.driver = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=self.options, service_args=['--verbose', '--log-path=/root/crawling/chromedriver.log'])

    
    def parse(self, response):
        self.driver.get(response.url)
        time.sleep(2.5)
        
        rows = self.driver.find_elements_by_xpath('//tr')
        myyield = {
            'kode':''
        }
        #yield { 'table' : rows.get() }
        i = 0
        with open(self.filename1, 'a') as f:
            for row in rows :
                i += 1
                if i > 6 :
                    print('save iterasi ke: ' + str(i))
                    data = row.find_elements_by_tag_name('td')
                    
                    if data[0] :
                        myyield['kode'] = data[0].text.encode('ascii', 'replace')
                    if data[1] :
                        myyield['tgl_registrasi'] = data[1].text.encode('ascii', 'replace')
                    if data[2] :
                        myyield['nama'] = data[2].text.encode('ascii', 'replace')
                    if data[3] :
                        myyield['jenis'] = data[3].text.encode('ascii', 'replace')
                    if data[4] :
                        myyield['kelas'] = data[4].text.encode('ascii', 'replace')
                    if data[5] :
                        myyield['direktur'] = data[5].text.encode('ascii', 'replace')
                    if data[6] :
                        myyield['alamat'] = data[6].text.encode('ascii', 'replace')
                    if data[7] :
                        myyield['penyelenggara'] = data[7].text.encode('ascii', 'replace')
                    if data[8] :
                        myyield['kab_kota'] = data[8].text.encode('ascii', 'replace')
                    if data[9] :
                        myyield['kodepos'] = data[9].text.encode('ascii', 'replace')
                    if data[10] :
                        myyield['telephone'] = data[10].text.encode('ascii', 'replace')
                    if data[11] :
                        myyield['fax'] = data[11].text.encode('ascii', 'replace')
                    if data[12] :
                        myyield['tgl_update'] = data[12].text.encode('ascii', 'replace')
                    
                    w = csv.DictWriter(f, self.fieldnames, lineterminator='\n', delimiter='|')
                    if i == 7 : 
                        w.writeheader()
                    w.writerow(myyield)
        time.sleep(2)
        self.driver.quit()