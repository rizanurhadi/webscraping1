# -*- coding: utf-8 -*-
import scrapy
import csv
import time



class Forlapdet1botSpider(scrapy.Spider):
    name = 'forlapdet1bot'
    allowed_domains = ['forlap.ristekdikti.go.id/perguruantinggi/detail']
    start_urls = ['https://forlap.ristekdikti.go.id/perguruantinggi/detail/NTJERDQ0MTEtREREMC00RkU2LUI1RUMtRjZGMzY3REJDRjk3']
    
    def start_requests(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        iterasi = 1
        with open('forlap_perguruan_tinggi.csv', 'r') as f:
            datareader = csv.reader(f)
            next(datareader)
            for row in datareader:
                iterasi += 1
                yield scrapy.Request(row[2], self.parse_profile, meta={'timestr':timestr,'iterasi':iterasi,'kode':row[0]})

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
        with open('forlap_pt_profile%s.csv' % response.meta.get('timestr'), 'a') as f:  # Just use 'w' mode in 3.x
            for row in response.css('table[class=table1] tr'):
                data = row.css('td')
                if data[2].css('::text').get() :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace("/", "_")]= data[2].css('::text').get().strip()
                else :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace("/", "_")]= ''
            
            for alltr in mytable[0].css('tr.ttop') :
                datatd = alltr.css('td')
                myyield['jml_dosen_tetap_1718'] = datatd[0].css('::text').get()
                myyield['jml_mhs_1718'] = datatd[1].css('::text').get()
                myyield['rasio_dosen_mhs_1718'] = datatd[2].css('::text').get()
                myyield['jml_dosen_tetap_1819'] = datatd[3].css('::text').get()
                myyield['jml_mhs_1819'] = datatd[4].css('::text').get()
                myyield['rasio_dosen_mhs_1819'] = datatd[5].css('::text').get()

            w = csv.DictWriter(f, myyield.keys(), lineterminator='\n')
            if response.meta.get('iterasi') ==2 : 
                w.writeheader()
            w.writerow(myyield)

        with open('forlap_pt_program_st%s.csv' % response.meta.get('timestr'), 'a') as f2:
            myitr = 0
            for alltr in mytable[1].css('tr') :
                myitr +=1
                if myitr > 2 :
                    datatd2 = alltr.css('td')
                    myprstudi['kode'] =datatd2[1].css('::text').get().strip()
                    myprstudi['nama'] =datatd2[2].css('::text').get().strip()
                    myprstudi['status'] =datatd2[3].css('::text').get().strip()
                    myprstudi['jenjang'] =datatd2[4].css('::text').get().strip()
                    myprstudi['jml_dosen_tetap_1718'] =datatd2[5].css('::text').get().strip()
                    myprstudi['jml_mhs_1718'] =datatd2[6].css('::text').get().strip()
                    myprstudi['rasio_dosen_mhs_1718'] =datatd2[7].css('::text').get().strip()
                    myprstudi['jml_dosen_tetap_1819'] =datatd2[8].css('::text').get().strip()
                    myprstudi['jml_mhs_1819'] =datatd2[9].css('::text').get().strip()
                    myprstudi['rasio_dosen_mhs_1819'] =datatd2[10].css('::text').get().strip()
                    w2 = csv.DictWriter(f2, myprstudi.keys(), lineterminator='\n')
                    if myitr ==3 : 
                        w2.writeheader()
                    w2.writerow(myprstudi)