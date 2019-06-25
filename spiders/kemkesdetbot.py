#! python2
# -*- coding: utf-8 -*-
import scrapy
import time
import csv
import os
from sys import exit
import logging 
from scrapy.utils.log import configure_logging  


class KemkesdetbotSpider(scrapy.Spider):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    configure_logging(install_root_handler = False) 
    logging.basicConfig ( 
        filename = dir_path + '/../out/log_kemkesdet.txt', 
        format = '%(levelname)s: %(message)s', 
        level = logging.WARNING 
    )
    name = 'kemkesdetbot'
    allowed_domains = ['sirs.yankes.kemkes.go.id']
    start_urls = ['http://sirs.yankes.kemkes.go.id/rsonline/data_view.php?editid1=']
    fieldnames = ['kode_rs','tgl_registrasi','rumah_sakit','jenis','kls_rs','direktur_rs','latar_belakang_pendidikan']
    
    def start_requests(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        iterasi = 1
        yield scrapy.Request(self.start_urls[0] + '1',meta={'iterasi':iterasi,'timestr':timestr,'page':1})

    def parse(self, response):
        #print(len(response.css('table[id=fields_block1] tr td::text')[1].get())
        if response.status == 200 :
            i=0
            #myyield = RumahSakitDet()
            myyield = {
                'update':''
            }
            
            with open(self.dir_path + '/../out/kemses_detail_%s.csv' % response.meta.get('timestr'), 'a') as f:
                iterasi = response.meta.get('iterasi')
                if response.css('table[id=fields_block1] tr td::text') :
                    for row in response.css('table[id=fields_block1] tr'):
                        data = row.css('td')
                        if i>=1 and i <9 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")] = data[1].css('::text').get().encode('utf-8').strip()
                        if i>=10 and i <18 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace("/", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                        if i>=19 and i <27 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                        if i>=28 and i <31 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                        if i >= 32 and i < 46 :
                            myyield['tempat_tidur_' + data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                        #Dokter
                        if i >= 48 and i < 63 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                            myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().encode('utf-8').strip()
                        #DokterGIgi
                        if i >= 64 and i < 69 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                            myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().encode('utf-8').strip()
                        #perawat
                        if i >= 70 and i < 74 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                            myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().encode('utf-8').strip()
                        #bidan
                        if i >= 75 and i < 77 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                            myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().encode('utf-8').strip()
                        #Keteknisian Medis
                        if i >= 78 and i < 88 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                        if i >= 78 and i < 87 :
                            myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().encode('utf-8').strip()
                        #Tenaga Kesehatan Lainnya
                        if i >= 89 and i < 94 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                            myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().encode('utf-8').strip()
                        #Tenaga Non Kesehatan
                        if i >= 95 and i < 102 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').strip()
                            myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().encode('utf-8').strip()
                        #Tenaga Non Kesehatan
                        if i >= 103 and i < 116 :
                            myyield['alatrs_'+ data[0].css('::text').get().split(':', 1)[0].strip().replace(" ", "_")]= data[0].css('::text').get().encode('utf-8').split(':')[1].strip()
                            #yield { 'test' : 'alatrs_'+ data[0].css('::text').get().split(':', 1)[0].strip().replace(" ", "_") + '= scrapy.Field()' }
                        if i >= 103 and i < 111 :
                            myyield['indikator_thn_sblm_'+ data[1].css('::text').get().split(':', 1)[0].strip().replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').split(':')[1].strip()
                            #yield { 'test' : 'indikator_thn_sblm_'+ data[1].css('::text').get().split(':', 1)[0].strip().replace(" ", "_") + '= scrapy.Field()' }
                        if i >= 103 and i < 107 :
                            myyield['lainya_'+ data[2].css('::text').get().split(':', 1)[0].strip().replace(" ", "_")]= data[2].css('::text').get().encode('utf-8').split(':')[1].strip()
                            #yield { 'test' : 'lainya_'+ data[2].css('::text').get().split(':', 1)[0].strip().replace(" ", "_") + '= scrapy.Field()' }
                        i += 1 
                        
                    #yield myyield
                    iterasi = response.meta.get('iterasi')
                    iterasi += 1
                    #handle if thereis new keys on fieldnames
                    self.fieldnames.extend(myyield.keys())
                    w = csv.DictWriter(f, self.fieldnames, lineterminator='\n', delimiter='|')
                    if response.meta.get('iterasi') ==1 : 
                        w.writeheader()
                    w.writerow(myyield)
                
                page = response.meta.get('page', 0) + 1
                #time.sleep(2)
                next_page = 'http://sirs.yankes.kemkes.go.id/rsonline/data_view.php?editid1=' + str(page)
                #exit(0) if page == 6 else None
                yield scrapy.Request(next_page, callback=self.parse, meta={'iterasi':iterasi,'timestr':response.meta.get('timestr'),'page': page })
