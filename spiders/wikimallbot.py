# -*- coding: utf-8 -*-
import scrapy
import csv
import time
from sys import exit


class WikimallbotSpider(scrapy.Spider):
    name = 'wikimallbot'
    allowed_domains = ['id.wikipedia.org/wiki/Daftar_pusat_perbelanjaan_di_Indonesia']
    start_urls = ['https://id.wikipedia.org/wiki/Daftar_pusat_perbelanjaan_di_Indonesia']

    def parse(self, response):
        #mw-headline
        timestr = time.strftime("%Y%m%d-%H%M%S")
        myyield = {'id_ai': 1}
        with open('wikimall_%s.csv' % timestr, 'a') as f:  # Just use 'w' mode in 3.x
            iterasi = 1
            rows = response.css('div.mw-parser-output')
            prov = ''
            kabkot = ''
            for row in rows.css('*') :
                #data = row.css('h2 > span.mw-headline')
                """for subrow in response.css('dl >  dt > a') :
                    #datasub = subrow.css('dt > a')
                    myyield['id_ai'] = iterasi
                    myyield['prov'] = data.css('::text').get()
                    myyield['kabkot'] = subrow.css('::text').get()
                """
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
                    if row.css('li') :
                        myyield['id_ai'] = iterasi
                        myyield['prov'] = prov
                        myyield['kabkot'] = kabkot
                        myyield['nama_mall'] = row.css('li *::text').get()
                        if row.css('li > a::attr(href)') :
                            detail_link = response.urljoin(row.css('li > a::attr(href)').get())
                            if 'index.php' not in detail_link :
                                myyield['detail_link'] = detail_link
                            else :
                                myyield['detail_link'] = ''
                        else :
                            myyield['detail_link'] = ''
                        #link_detail = response.urljoin(link_detail)
                        iterasi += 1
                        subiterasi += 1
                        w = csv.DictWriter(f, myyield.keys(), lineterminator='\n')
                        if iterasi ==2 : 
                            w.writeheader()
                        w.writerow(myyield)
                        
                        #if iterasi == 50 :
                        #    exit(0)
    def parse_profile(self, response) :
        myyield = response.meta.get('myyield')
        iterasi = response.meta.get('iterasi')
        f = response.meta.get('f')
        if response.css('table.infobox tr') :
            rows = response.css('table.infobox tr')
            for row in rows :
                myyield[row.css('th::text').get().lower().replace(" ", "_").replace("/", "_")] = myyield[row.css('td a::text').get()]
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
        if iterasi ==2 : 
            w.writeheader()
        w.writerow(myyield)
        yield myyield
        #subiterasi = response.meta.get('subiterasi')
       