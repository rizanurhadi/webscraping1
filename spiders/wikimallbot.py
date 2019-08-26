#! python2
# -*- coding: utf-8 -*-
import scrapy
import csv
import time
from sys import exit
import os 
import logging 
from scrapy import signals
from . import wikimallbottodbmy
import re
#from scrapy.utils.log import configure_logging  


class WikimallbotSpider(scrapy.Spider):
    name = 'wikimallbot'
    allowed_domains = ['id.wikipedia.org']
    start_urls = ['https://id.wikipedia.org/wiki/Daftar_pusat_perbelanjaan_di_Indonesia']
    dir_path = os.path.dirname(os.path.realpath(__file__))
    #configure_logging(install_root_handler = False) 
    #logging.basicConfig ( 
    #    filename = dir_path + '/../out/wikimall_log.txt', 
    #    format = '%(levelname)s: %(message)s', 
    #    level = logging.WARNING 
    #)
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename1 = dir_path + '/../out/wikimall_%s.csv' % timestr
    filename2 = dir_path + '/../out/wikimall_detail_%s.csv' % timestr
    filename3 = dir_path + '/../out/wikimall_links.csv'
    fieldnames = ['id_ai','prov','kabkot','nama_mall','detail_link']
    fieldnames_detail = ['nama_mall','alamat','lokasi','pemilik','pengembang','pengurus','tanggal_dibuka','jumlah_toko_dan_jasa','jumlah_toko_induk','total_luas_pertokoan','jumlah_lantai','parkir','situs_web','kantor','didirikan','industri','akses_transportasi_umum','pendapatan','arsitek']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WikimallbotSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider):
        spider.logger.info('Signal sent then Spider closed. file out is : %s', self.filename1)
        #self.connect()
        #bumntodb.readcsvandupdate(self.allowed_domains[0],self.filename1)
        wikimallbottodbmy.readcsvandupdate(self.allowed_domains[0],self.filename1)
        wikimallbottodbmy.readcsvandupdate(self.allowed_domains[0],self.filename2)
        # saving to mysql should load here

    def parse(self, response):
        #mw-headline
        
        myyield = {'id_ai': 1}
        open(self.filename3, 'a').close()

        with open(self.filename2, 'a') as f: 
            w = csv.DictWriter(f, self.fieldnames_detail, lineterminator='\n', delimiter='|')
            w.writeheader()

        with open(self.filename1, 'a') as f:  # Just use 'w' mode in 3.x
            iterasi = 1
            rows = response.css('div.mw-parser-output')
            prov = ''
            kabkot = ''
            for row in rows.css('*') :
                if row.xpath('name()').get() == 'h2' :
                    #myyield['id_ai'] = iterasi
                    myyield['prov'] = row.css('::text').get()
                    prov = row.css('::text').get()
                    #myyield['test'] = row.css('::text').get()
                    subiterasi = 1
                if row.xpath('name()').get() == 'dl' :
                    if row.css('dt > a::text') :
                        myyield['id_ai'] = subiterasi
                        myyield['prov'] = prov
                        myyield['kabkot'] = row.css('dt > a::text').get()
                        kabkot = row.css('dt > a::text').get()

                if row.xpath('name()').get() == 'li' :
                    if row.css('li') and row.css('li *::text') :
                        myyield['id_ai'] = iterasi
                        myyield['prov'] = prov.encode('utf-8')
                        myyield['kabkot'] = kabkot.encode('utf-8')
                        myyield['nama_mall'] = re.sub(r'[^\x00-\x7F]+',' ', (row.css('li *::text').get().encode('utf-8')))
                        if row.css('li > a::attr(href)') :
                            detail_link = response.urljoin(row.css('li > a::attr(href)').get().encode('utf-8'))
                            if 'index.php' not in detail_link :
                                myyield['detail_link'] = detail_link.encode('utf-8')
                                #yield scrapy.Request(detail_link.encode('utf-8'), self.parse_detail, meta={'timestr':timestr,'iterasi':iterasi,'row':myyield})
                                #with open(self.dir_path + '/../out/wikimall_links.csv', 'a') as f2:
                                #    w2 = csv.DictWriter(f2, self.fieldnames, lineterminator='\n', delimiter='|')
                                #    w2.writerow(myyield)
                            else :
                                myyield['detail_link'] = ''
                        else :
                            myyield['detail_link'] = ''
                        #link_detail = response.urljoin(link_detail)
                        iterasi += 1
                        subiterasi += 1
                        w = csv.DictWriter(f, self.fieldnames, lineterminator='\n', delimiter='|')
                        if iterasi ==2 : 
                            w.writeheader()
                        w.writerow(myyield)
                        with open(self.filename3, 'a') as f2:
                            w2 = csv.DictWriter(f2, self.fieldnames, lineterminator='\n', delimiter='|')
                            if iterasi ==2 : 
                                w2.writeheader()
                            w2.writerow(myyield)
        
        for link in response.css('div.mw-parser-output li > a::attr(href)').getall() :
            if 'index.php' not in link :
                if ':' not in link.encode('utf-8') :
                    yield scrapy.Request(response.urljoin(link.encode('utf-8')), self.parse_detail)

    #def parse_detail(self, response) :
    #    print(response.css('table.infobox tr').get())

    def parse_detail(self,response) :
        myyield = {'nama_mall': response.css('h1.firstHeading::text').get()}
        with open(self.filename2, 'a') as f: 
            if response.css('table.infobox tr') :
                rows = response.css('table.infobox tr')
                for row in rows :
                    if row.css('th::text') and row.css('td *::text') :
                        #self.log('key file %s' % row.css('th::text').get())
                        if row.css('th::text').get().encode('utf-8').lower().replace(" ", "_").replace("/", "_").replace(",", "||") in self.fieldnames_detail :
                            if len(row.css('td *::text').getall()) > 1 :
                                myyield[row.css('th::text').get().encode('utf-8').lower().replace(" ", "_").replace("/", "_").replace(",", "||")] = re.sub(r'[^\x00-\x7F]+',' ', (' '.join(t.encode('utf-8').replace("\n", "").strip() for t in  row.css('td *::text').getall()).strip()))
                            else :
                                myyield[row.css('th::text').get().encode('utf-8').lower().replace(" ", "_").replace("/", "_").replace(",", "||")] = re.sub(r'[^\x00-\x7F]+',' ', (row.css('td *::text').get().encode('utf-8').replace("\n", "")))
                        
            else : 
                myyield['alamat'] = ''
                myyield['lokasi'] = ''
                myyield['pemilik'] = ''
                myyield['pengembang'] = ''
                myyield['pengurus'] = ''
                myyield['tanggal_dibuka'] = ''
                myyield['jumlah_toko_dan_jasa'] = ''
                myyield['jumlah_toko_induk'] = ''
                myyield['total_luas_pertokoan'] = ''
                myyield['jumlah_lantai'] = ''
                myyield['parkir'] = ''
                myyield['situs_web'] = ''
                myyield['kantor'] = ''
                myyield['didirikan'] = ''
                myyield['industri'] = ''
                myyield['akses_transportasi_umum'] = ''
                myyield['pendapatan'] = ''
                myyield['arsitek'] = ''
                
            w = csv.DictWriter(f, self.fieldnames_detail, lineterminator='\n', delimiter='|')
            #if response.meta.get('iterasi') ==2 : 
            #    w.writeheader()
            w.writerow(myyield)