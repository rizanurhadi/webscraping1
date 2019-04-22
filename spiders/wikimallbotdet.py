# -*- coding: utf-8 -*-
import scrapy
import csv
import time
from sys import exit

class WikimallbotdetSpider(scrapy.Spider):
    name = 'wikimallbotdet'
    allowed_domains = ['id.wikipedia.org/wiki/Daftar_pusat_perbelanjaan_di_Indonesia']
    start_urls = ['https://id.wikipedia.org/wiki/Daftar_pusat_perbelanjaan_di_Indonesia']

    def start_requests(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        iterasi = 1
        with open('wikimall_links.csv', 'r') as f:
            datareader = csv.reader(f)
            next(datareader)
            for row in datareader:
                iterasi += 1
                myyield = {"id_ai": row[0],"prov": row[1],"kabkot": row[2],"nama_mall": row[3]}
                if row[4] :
                    yield scrapy.Request(row[4], self.parse, meta={'timestr':timestr,'iterasi':iterasi,'row':row})
                else :
                    myyield['alamat'] = ''
                    myyield['lokasi'] = ''
                    myyield['pemilik'] = ''
                    myyield['pengembang'] = ''
                    myyield['pengurus'] = ''
                    myyield['tanggal_dibuka'] = ''
                    myyield['jumlah_toko_dan_jasa'] = ''
                    myyield['jumlah_toko_dan_jasa'] = ''
                    myyield['jumlah_toko_induk'] = ''
                    myyield['total_luas_pertokoan'] = ''
                    myyield['jumlah_lantai'] = ''
                    myyield['parkir'] = ''
                    myyield['situs_web'] = ''
                    with open('wikimall_profile_%s.csv' % timestr, 'a') as myf: 
                        w = csv.DictWriter(myf, myyield.keys(), lineterminator='\n')
                        if iterasi ==2 : 
                            w.writeheader()
                        w.writerow(myyield)


    def parse(self, response):
        mainrow = response.meta.get('row')
        myyield = {"id_ai": mainrow[0],"prov": mainrow[1],"kabkot": mainrow[2],"nama_mall": mainrow[3]}

        with open('wikimall_profile_%s.csv' % response.meta.get('timestr'), 'a') as f: 
            if response.css('table.infobox tr') :
                rows = response.css('table.infobox tr')
                for row in rows :
                    if row.css('th::text') and row.css('td *::text') :
                        #self.log('key file %s' % row.css('th::text').get())
                        myyield[row.css('th::text').get().lower().replace(" ", "_").replace("/", "_").replace(",", "||")] = row.css('td *::text').get().replace("\n", "")
            else : 
                myyield['alamat'] = ''
                myyield['lokasi'] = ''
                myyield['pemilik'] = ''
                myyield['pengembang'] = ''
                myyield['pengurus'] = ''
                myyield['tanggal_dibuka'] = ''
                myyield['jumlah_toko_dan_jasa'] = ''
                myyield['jumlah_toko_dan_jasa'] = ''
                myyield['jumlah_toko_induk'] = ''
                myyield['total_luas_pertokoan'] = ''
                myyield['jumlah_lantai'] = ''
                myyield['parkir'] = ''
                myyield['situs_web'] = ''
            w = csv.DictWriter(f, myyield.keys(), lineterminator='\n')
            if response.meta.get('iterasi') ==2 : 
                w.writeheader()
            w.writerow(myyield)
