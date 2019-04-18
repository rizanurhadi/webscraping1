# -*- coding: utf-8 -*-
import scrapy
import time
from sys import exit

class RumahSakitDet(scrapy.Item): 
    kode = scrapy.Field() 
    tgl_registrasi = scrapy.Field() 
    nama = scrapy.Field() 
    jenis = scrapy.Field()
    kelas = scrapy.Field()
    direktur = scrapy.Field()
    direktur_pendidikan = scrapy.Field()
    pemilik = scrapy.Field()
    alamat = scrapy.Field()
    kab_kota = scrapy.Field()
    kodepos = scrapy.Field()
    tlp = scrapy.Field()
    fax = scrapy.Field()
    email = scrapy.Field()
    tlp_humas = scrapy.Field()
    website = scrapy.Field()
    luas_tanah= scrapy.Field()
    luas_bangunan= scrapy.Field()
    no_surat_ijin= scrapy.Field()
    tanggal_surat_ijin= scrapy.Field()
    surat_ijin_dari= scrapy.Field()
    sifat_surat_ijin= scrapy.Field()
    masa_berlaku_surat_ijin= scrapy.Field()
    status_penyelenggara= scrapy.Field()
    status_akreditasi= scrapy.Field()
    tgl_akreditas= scrapy.Field()
    berlaku_sampai_dengan= scrapy.Field()
    

class KemkesdetbotSpider(scrapy.Spider):
    name = 'kemkesdetbot'
    allowed_domains = ['sirs.yankes.kemkes.go.id']
    start_urls = ['http://sirs.yankes.kemkes.go.id/rsonline/data_view.php?editid1=']
    
    def start_requests(self):
        yield scrapy.Request(self.start_urls[0] + '1',meta={'page':1})

    def parse(self, response):
        if response.status == 200 :
            i=0
            #myyield = RumahSakitDet()
            myyield = {
                'update':''
            }
            for row in response.css('table[id=fields_block1] tr'):
                data = row.css('td')
                if i>=1 and i <9 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")] = data[1].css('::text').get().strip()
                if i>=10 and i <18 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace("/", "_")]= data[1].css('::text').get().strip()
                if i>=19 and i <27 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                if i>=28 and i <31 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                if i >= 32 and i < 46 :
                    myyield['tempat_tidur_' + data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                #Dokter
                if i >= 48 and i < 63 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                    myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().strip()
                #DokterGIgi
                if i >= 64 and i < 69 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                    myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().strip()
                #perawat
                if i >= 70 and i < 74 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                    myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().strip()
                #bidan
                if i >= 75 and i < 77 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                    myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().strip()
                #Keteknisian Medis
                if i >= 78 and i < 88 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                if i >= 78 and i < 87 :
                    myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().strip()
                #Tenaga Kesehatan Lainnya
                if i >= 89 and i < 94 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                    myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().strip()
                #Tenaga Non Kesehatan
                if i >= 95 and i < 102 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_")]= data[1].css('::text').get().strip()
                    myyield[data[2].css('::text').get().lower().replace(" ", "_")]= data[3].css('::text').get().strip()
                #Tenaga Non Kesehatan
                if i >= 103 and i < 116 :
                    myyield['alatrs_'+ data[0].css('::text').get().split(':', 1)[0].strip().replace(" ", "_")]= data[0].css('::text').get().split(':')[1].strip()
                    #yield { 'test' : 'alatrs_'+ data[0].css('::text').get().split(':', 1)[0].strip().replace(" ", "_") + '= scrapy.Field()' }
                if i >= 103 and i < 111 :
                    myyield['indikator_thn_sblm_'+ data[1].css('::text').get().split(':', 1)[0].strip().replace(" ", "_")]= data[1].css('::text').get().split(':')[1].strip()
                    #yield { 'test' : 'indikator_thn_sblm_'+ data[1].css('::text').get().split(':', 1)[0].strip().replace(" ", "_") + '= scrapy.Field()' }
                if i >= 103 and i < 107 :
                    myyield['lainya_'+ data[2].css('::text').get().split(':', 1)[0].strip().replace(" ", "_")]= data[2].css('::text').get().split(':')[1].strip()
                    #yield { 'test' : 'lainya_'+ data[2].css('::text').get().split(':', 1)[0].strip().replace(" ", "_") + '= scrapy.Field()' }
                i += 1 
                """if len(data) == 2 :
                    yield {
                        data[0].css('::text').get() : data[1].css('::text').get() 
                    }
                if len(data) == 4 :
                    yield {
                        data[0].css('::text').get() : data[1].css('::text').get(),
                        data[2].css('::text').get() : data[3].css('::text').get(),
                    }
                """
                """if len(data) > 1 :
                    if i==1 :
                        myyield['kode'] = data[1].css('::text').get() 
                    if i==2 :
                        myyield['tgl_registrasi'] = data[1].css('::text').get()
                    i += 1 
            yield myyield
            """
            yield myyield
            page = response.meta.get('page', 0) + 1
            #time.sleep(2)
            next_page = 'http://sirs.yankes.kemkes.go.id/rsonline/data_view.php?editid1=' + str(page)
            #exit(0) if page == 6 else None
            yield scrapy.Request(next_page, callback=self.parse, meta={'page': page })
