#! python2
# -*- coding: utf-8 -*-
import scrapy
import csv
import time
import os
import re
from scrapy import signals
from . import forlapdetproftodbmy
from . import forlapdetprgsttodbmy


class Forlapdet1botSpider(scrapy.Spider):
    name = 'forlapdet1bot'
    allowed_domains = ['forlap.ristekdikti.go.id/perguruantinggi/detail']
    start_urls = ['https://forlap.ristekdikti.go.id/perguruantinggi/detail/NTJERDQ0MTEtREREMC00RkU2LUI1RUMtRjZGMzY3REJDRjk3']
    dir_path = os.path.dirname(os.path.realpath(__file__))
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename1 = dir_path + '/../out/forlap_pt_profile%s.csv' % timestr
    filename2 = dir_path + '/../out/forlap_perguruan_tinggi.csv'
    #filename2 = dir_path + '/../out/forlap_pt_test.csv'
    filename3 = dir_path + '/../out/forlap_pt_program_st%s.csv' % timestr
    #profile
    fieldnames = ['kode','status_pt','tanggal_sk_pt','tanggal_berdiri','perguruan_tinggi','website','telepon','kode_pos','email','faximile','kota_kabupaten','alamat','nomor_sk_pt','jml_mhs_1718','jml_dosen_tetap_1718','rasio_dosen_mhs_1718','jml_mhs_1819','jml_dosen_tetap_1819','rasio_dosen_mhs_1819']
    #programstudi
    fieldnames2 = ['kodept','kode','nama','status','jenjang','jml_dosen_tetap_1718','jml_mhs_1718','rasio_dosen_mhs_1718','jml_dosen_tetap_1819','jml_mhs_1819','rasio_dosen_mhs_1819']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(Forlapdet1botSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider):
        spider.logger.info('Signal sent then Spider closed. file out is : %s', self.filename1)
        #save to db here
        forlapdetproftodbmy.readcsvandupdate(self.allowed_domains[0],self.filename1)
        forlapdetprgsttodbmy.readcsvandupdate(self.allowed_domains[0],self.filename3)
    
    def start_requests(self):
        iterasi = 1
        csv.register_dialect('myDialect',delimiter='|')
        with open(self.filename2, 'r') as f:
            datareader = csv.reader(f,dialect = 'myDialect')
            next(datareader)
            for row in datareader:
                iterasi += 1
                yield scrapy.Request(row[2], self.parse_profile, meta={'iterasi':iterasi,'kode':row[0]})
                #if iterasi == 5 :
                #    exit(0)

    def parse(self, response):
        """
        filename = 'testdetail.txt'
        with open(filename, 'w') as f:
            for row in response.css('table[class=table1] tr'):
                data = row.css('td')
                #myyield[data[0].css('::text').get().lower().replace(" ", "_").replace("/", "_")]= data[2].css('::text').get().strip()
                f.writelines('test1' + data[2].css('::text').get().strip())
        #self.log('Saved file %s' % filename)
        
        iterasi = 0
        myyield = {
                'iterasi':iterasi
            }
        for row in response.css('table[class=table1] tr'):
                data = row.css('td')
                iterasi += 1
                myyield['iterasi'] = iterasi
                myyield[data[0].css('::text').get().lower().replace(" ", "_").replace("/", "_")]= data[2].css('::text').get().strip()
        yield myyield
        """
    def parse_profile(self, response):
        #self.log('profile parsing %s' % response.url)
        myyield = {"kode": response.meta.get('kode')}
        myprstudi = {"kodept": response.meta.get('kode')}
        #ptdetail
        mytable = response.xpath('//table[@class="table table-bordered"]') 
        with open(self.filename1, 'a') as f:  # Just use 'w' mode in 3.x
            for row in response.css('table[class=table1] tr'):
                data = row.css('td')
                if data[2].css('::text').get() :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace("/", "_")]= re.sub(r'[^\x00-\x7F]+','',data[2].css('::text').get().replace('\n', ' ').replace('\r', ' ').strip())
                else :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace("/", "_")]= ''
            
            for alltr in mytable[0].css('tr.ttop') :
                datatd = alltr.css('td')
                myyield['jml_dosen_tetap_1718'] = re.sub(r'[^\x00-\x7F]+','', datatd[0].css('::text').get())
                myyield['jml_mhs_1718'] = re.sub(r'[^\x00-\x7F]+','',datatd[1].css('::text').get())
                myyield['rasio_dosen_mhs_1718'] = re.sub(r'[^\x00-\x7F]+','',datatd[2].css('::text').get())
                myyield['jml_dosen_tetap_1819'] = re.sub(r'[^\x00-\x7F]+','',datatd[3].css('::text').get())
                myyield['jml_mhs_1819'] = re.sub(r'[^\x00-\x7F]+','',datatd[4].css('::text').get())
                myyield['rasio_dosen_mhs_1819'] = re.sub(r'[^\x00-\x7F]+','',datatd[5].css('::text').get())

            w = csv.DictWriter(f, self.fieldnames, lineterminator='\n', delimiter='|')
            if response.meta.get('iterasi') ==2 : 
                w.writeheader()
            w.writerow(myyield)

        with open(self.filename3, 'a') as f2:
            myitr = 0
            for alltr in mytable[1].css('tr') :
                myitr +=1
                if myitr > 2 :
                    datatd2 = alltr.css('td')
                    myprstudi['kode'] =re.sub(r'[^\x00-\x7F]+','',datatd2[1].css('::text').get().strip())
                    myprstudi['nama'] =re.sub(r'[^\x00-\x7F]+','',datatd2[2].css('::text').get().strip())
                    myprstudi['status'] =re.sub(r'[^\x00-\x7F]+','',datatd2[3].css('::text').get().strip())
                    myprstudi['jenjang'] =re.sub(r'[^\x00-\x7F]+','',datatd2[4].css('::text').get().strip())
                    myprstudi['jml_dosen_tetap_1718'] =re.sub(r'[^\x00-\x7F]+','',datatd2[5].css('::text').get().strip())
                    myprstudi['jml_mhs_1718'] =re.sub(r'[^\x00-\x7F]+','',datatd2[6].css('::text').get().strip())
                    myprstudi['rasio_dosen_mhs_1718'] =re.sub(r'[^\x00-\x7F]+','',datatd2[7].css('::text').get().strip())
                    myprstudi['jml_dosen_tetap_1819'] =re.sub(r'[^\x00-\x7F]+','',datatd2[8].css('::text').get().strip())
                    myprstudi['jml_mhs_1819'] =re.sub(r'[^\x00-\x7F]+','',datatd2[9].css('::text').get().strip())
                    myprstudi['rasio_dosen_mhs_1819'] =re.sub(r'[^\x00-\x7F]+','',datatd2[10].css('::text').get().strip())
                    w2 = csv.DictWriter(f2, self.fieldnames2, lineterminator='\n', delimiter='|')
                    if myitr ==3 : 
                        w2.writeheader()
                    w2.writerow(myprstudi)