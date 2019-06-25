#! python2
# -*- coding: utf-8 -*-
import scrapy
import time
import csv
import os 
from selenium import webdriver
import logging 
from scrapy.utils.log import configure_logging  

class KemsesbotSpider(scrapy.Spider):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    configure_logging(install_root_handler = False) 
    logging.basicConfig ( 
        filename = dir_path + '/../out/log_kemkes.txt', 
        format = '%(levelname)s: %(message)s', 
        level = logging.INFO 
    )
    name = 'kemsesbot'
    allowed_domains = ['sirs.yankes.kemkes.go.id']
    start_urls = ['http://sirs.yankes.kemkes.go.id/rsonline/DATA_RUMAH_SAKIT_REPORT_report.php?pagesize=-1/']
    fieldnames = ['kode','tgl_registrasi','nama','jenis','kelas','direktur','alamat','penyelenggara','kab_kota','kodepos','telephone','fax','tgl_update','produk']

    def __init__(self):
        self.driver = webdriver.Chrome(executable_path='D:/scrapy_script/chromedriver_win32/chromedriver.exe')
    
    def parse(self, response):
        self.driver.get(response.url)
        time.sleep(2.5)
        timestr = time.strftime("%Y%m%d-%H%M%S")
        rows = self.driver.find_elements_by_xpath('//tr')
        #yield { 'table' : rows.get() }
        i = 0
        with open(self.dir_path + '/../out/kemkesbot_%s.csv' % timestr, 'a') as f:
            for row in rows :
                i += 1
                if i > 6 :
                    print('save iterasi ke: ' + str(i))
                    data = row.find_elements_by_tag_name('td')
                    myyield = {
                        'kode' : data[0].text.encode('ascii', 'replace'),
                        'tgl_registrasi':data[1].text.encode('ascii', 'replace'),
                        'nama':data[2].text.encode('ascii', 'replace'),
                        'jenis':data[3].text.encode('ascii', 'replace'),
                        'kelas':data[4].text.encode('ascii', 'replace'),
                        'direktur':data[5].text.encode('ascii', 'replace'),
                        'alamat':data[6].text.encode('ascii', 'replace'),
                        'penyelenggara':data[7].text.encode('ascii', 'replace'),
                        'kab_kota':data[8].text.encode('ascii', 'replace'),
                        'kodepos':data[9].text.encode('ascii', 'replace'),
                        'telephone':data[10].text.encode('ascii', 'replace'),
                        'fax':data[11].text.encode('ascii', 'replace'),
                        'tgl_update':data[12].text.encode('ascii', 'replace'),
                        
                    }
                    w = csv.DictWriter(f, self.fieldnames, lineterminator='\n', delimiter='|')
                    if i == 7 : 
                        w.writeheader()
                    w.writerow(myyield)
        time.sleep(2)
        self.driver.quit()