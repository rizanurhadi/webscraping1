#! python2
# -*- coding: utf-8 -*-
import scrapy
import time
import csv
from sys import exit


class KemendagbotSpider(scrapy.Spider):
    name = 'kemendagbot'
    allowed_domains = ['kemendag.go.id']
    start_urls = ['http://www.kemendag.go.id/id/perdagangan-kita/company-directory/data-center-collection/?N=']

    def start_requests(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        iterasi = 1
        yield scrapy.Request(self.start_urls[0] + '1',meta={'page':1,'timestr':timestr,'iterasi':iterasi})

    def parse(self, response):
        with open('kemendag_%s.csv' % response.meta.get('timestr'), 'a') as f: 
            iterasi = response.meta.get('iterasi')
            
            page = response.meta.get('page', 0) + 1
            myyield = {
                    'no':1
                }
            fieldnames = ['no', 'nama_perusahaan', 'kontak', 'posisi', "alamat", "ph", "fax","kecamatan","kabupaten","provinsi","product"]
            for row in response.css('table.statistik tr'):
                if row.css('td') :
                    iterasi += 1
                    data = row.css('td')
                    myyield['no'] = data[0].css('::text').get()
                    myyield['nama_perusahaan'] = data[1].css('h1::text').get()
                    myyield['kontak'] = myyield['posisi'] = myyield['ph'] = myyield['fax'] = myyield['alamat'] = myyield['kecamatan'] = myyield['kabupaten'] = myyield['provinsi'] =  myyield['product'] = ''
                    if data[1].css('p') :
                        for subrow in data[1].css('p') :
                            myyield[subrow.css('::text').get().split(':')[0].strip().lower()] = subrow.css('::text').get().split(':')[1].strip()
                    subiterasi = 1
                    for subrow in data[2].css('p') :
                        if subiterasi == 1 :
                            myyield['alamat'] = subrow.css('::text').get()
                        else :
                            myyield[subrow.css('::text').get().split(':')[0].strip().lower()] = subrow.css('::text').get().split(':')[1].strip()
                        subiterasi += 1
                    myyield['product'] = data[3].css('::text').get()
                    w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator='\n', delimiter='|')
                    if iterasi ==2 : 
                        w.writeheader()
                    w.writerow(myyield)
            next_page = self.start_urls[0] + str(page)
            if page == 1110 :
                exit(0)
            yield scrapy.Request(next_page, callback=self.parse, meta={'page': page,'timestr':response.meta.get('timestr'),'iterasi':iterasi })
