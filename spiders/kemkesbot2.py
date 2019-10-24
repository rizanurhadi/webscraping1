# -*- coding: utf-8 -*-
import scrapy
import time
import csv
import os 
from selenium import webdriver
import logging 
import re
from selenium.webdriver.chrome.options import Options
#from scrapy.utils.log import configure_logging  
from scrapy import signals
from . import kemkesbottodbmy
from . import kemkesdetbottodbmy


class Kemkesbot2Spider(scrapy.Spider):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    name = 'kemkesbot2'
    allowed_domains = ['sirs.yankes.kemkes.go.id']
    start_urls = ['http://sirs.yankes.kemkes.go.id/rsonline/data_list.php']
    fieldnames = ['kode','nama','tgl_registrasi','jenis','kelas','direktur','pemilik','alamat','penyelenggara','kab_kota','kodepos','telephone','fax','tgl_update','email']
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename1 = dir_path + '/../out/kemkesbot_%s.csv' % timestr
    filename2 = dir_path + '/../out/kemses_detail_%s.csv' % timestr
    fieldnames2 = ['kode_rs','rumah_sakit','tgl_registrasi','jenis','kls_rs','direktur_rs','latar_belakang_pendidikan','pemilik','alamat','kode_pos','telepon','fax','email','telepon_humas','website','status_akreditasi']
    fieldnames2.extend([ 'luas_tanah', 'berlaku_sampai_dengan', 'kab_kota', 'tgl_akreditas', 'tanggal_surat_ijin', 'surat_ijin_dari', 'no_surat_ijin', 'sifat_surat_ijin', 'status_penyelenggara', 'luas_bangunan', 'update', 'masa_berlaku_surat_ijin'])
    #,'bidan_pendidik','dokter_gigi_sp_gigi_tiruan','pekarya','alatrs_Meja_Operasi','dokter_sp_bedah_plastik','kesja','analis_farmasi','tempat_tidur_tt_di_ruang_isolasi','humas','perawat_maternitas','alatrs_CT_Scan','berlaku_sampai_dengan','rumah_sakit','entomologi','dietisien','biostatistik','informasi_kesehatan','dokter_sp_lainnya','widyaiswara','luas_bangunan','akupunturis','sanitasi','tempat_tidur_igd','alatrs_E_E_G','tempat_tidur_hcu','jenis','dokter_sub_spesialis','fisioterapi','lainya_SIMRS','administrasi_keuangan'
    #,'dokter_sp_bedah_orthopedi','indikator_thn_sblm_T_O_I','status_penyelenggara','dokter_gigi_sp_periodonsia','tempat_tidur_nicu','dokter_sp_psikiatri','dokter_sp_bedah_saraf','dokter_sp_orthopedi','dokter_gigi_sp_konservasi','tanggal_surat_ijin','dokter_sp_og','kode_rs','perawat_komunitas','alatrs_E_K_G','tempat_tidur_vip','dokter_gigi_sp_radiologi','informasi_teknologi','surat_ijin_dari','sifat_surat_ijin','alatrs_Defibrilator','indikator_thn_sblm_N_D_R','no_surat_ijin','kesehatan_lingkungan','dokter_sp_tht','kab_kota','tempat_tidur_kelas_iii'
    #,'dokter_sp_jp','perawat_bedah','alatrs_Inkubator','dokter_gigi_sp_bedah_mulut','dokter_sp_paru','nutrisionis','rekam_medik','epidemiologi','dokter_sp_patologi_anatomi','latar_belakang_pendidikan','indikator_thn_sblm_Rawat_Jalan','dokter_sp_saraf','refraksionis','dokter_sp_rm','analis_kesehatan','dokter_sp_bedah_anak','dokter_sp_ofthalmologi','teknisi_gigi','apoteker','radiografer','dokter_sp_a','dokter_sp_forensik','dokter_gigi_sp_karang_gigi','tempat_tidur_iccu','dokter_gigi_sp_penyakit_mulut','dokter_sp_m','tempat_tidur_kelas_ii','terapi_wicara'
    #,'tenaga_non_kes','indikator_thn_sblm_A_L_O_S','teknisi_transfusi_darah','perilaku','dokter_sp_kulit_dan_kelamin','masa_berlaku_surat_ijin','alatrs_U_S_G','perpustakaan','alatrs_Ventilator','direktur_rs','indikator_thn_sblm_I_G_D','lainya_Ambulan','tempat_tidur_icu','promosi_kesehatan','dokter_sp_urologi','dokter_gigi','alatrs_Mesin_Anestesi','alatrs_X-Ray','dokter_sp_bedah_thoraks','dosen','elektromedis','tempat_tidur_tt_bayi_baru_lahir','lainya_Bank_Darah','mikrobiologi','dokter_sp_pd','dokter_sp_rad','perawat_lainnya','dokter_sp_pk','alatrs_Autoclav'
    #,'dr_umum','dokter_gigi_sp_lainnya','perawat_anak','indikator_thn_sblm_Rawat_Inap','indikator_thn_sblm_G_D_R','teknisi_kardiovaskular','perawat_gigi','tgl_akreditas','kls_rs','perawat_anestesi','ners','luas_tanah','administrasi_kesehatan','alatrs_Blue_Light','alatrs_M_R_I','hukum','ortotik','dokter_gigi_sp_anak','jaminan_kesehatan','kesmas_lainnya','tgl_registrasi','dokter_sp_b','tempat_tidur_picu','indikator_thn_sblm_B_O_R','dokter_sp_okupasi','tempat_tidur_tt_di_ruang_operasi','update','lainya_Layanan_Unggulan','dokter_sp_an','perencanaan','tempat_tidur_vvip'
    #,'terapi_okupasi','dokter_sp_kes._jiwa','radioterapis','psikologi','program_kesehatan','bidan_klinik','reproduksi','pelaporan','tempat_tidur_kelas_i'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(Kemkesbot2Spider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider):
        spider.logger.info('Signal sent then Spider closed. file out is : %s', self.filename1)
        spider.logger.info('Signal sent then Spider closed. file out is : %s', self.filename2)
        #save to db here
        kemkesbottodbmy.readcsvandupdate(self.allowed_domains[0],self.filename1)
        kemkesdetbottodbmy.readcsvandupdate(self.allowed_domains[0],self.filename2)

    def parse(self, response):
        total = response.xpath('//span[@id="detFound1"]/text()').get().split(" ")[2]
        seturl = self.start_urls[0] + '?pagesize=' + total
        #seturl = self.start_urls[0]
        #print(response.xpath('//form[@id="frmAdmin1"]').css('div table tr td').get())
        with open(self.filename2, 'a') as f:
                w = csv.DictWriter(f, self.fieldnames2, lineterminator='\n', delimiter='|')
                w.writeheader()
        yield scrapy.Request(seturl, self.parse_list,meta={})
    
    def parse_list(self, response):
        iterasi = 1
        for row in response.xpath('//form[@id="frmAdmin1"]').css('div table tbody').xpath('//tr[boolean(@rowid)]') :
            myfield = row.css('td')
            link_detail = 'http://sirs.yankes.kemkes.go.id/rsonline/' +  myfield[0].css('a::attr(href)').get()
            yield scrapy.Request(link_detail, self.parse_detail,meta={})
            kode = '' 
            if myfield[1].css('p span::text').get() :
                kode = myfield[1].css('p span::text').get().strip()
            tgl_registrasi = '' 
            if myfield[2].css('p span::text').get() :
                tgl_registrasi = myfield[2].css('p span::text').get().strip()
            nama = '' 
            if myfield[3].css('p span::text').get() :
                nama = myfield[3].css('p span::text').get().strip()
            jenis = '' 
            if myfield[4].css('p span::text').get() :
                jenis = myfield[4].css('p span::text').get().strip()
            kelas = '' 
            if myfield[5].css('p span::text').get() :
                kelas = myfield[5].css('p span::text').get().strip()
            direktur = '' 
            if myfield[6].css('p span::text').get() :
                direktur = myfield[6].css('p span::text').get().strip()
            pemilik = '' 
            if myfield[7].css('p span::text').get() :
                pemilik = myfield[7].css('p span::text').get().strip()
            alamat = '' 
            if myfield[8].css('p span::text').get() :
                alamat = myfield[8].css('p span::text').get().strip()
            kab_kota = '' 
            if myfield[9].css('p span::text').get() :
                kab_kota = myfield[9].css('p span::text').get().strip()
            kodepos = '' 
            if myfield[10].css('p span::text').get() :
                kodepos = myfield[10].css('p span::text').get().strip()
            telephone = ''
            if myfield[11].css('p span::text').get() :
                telephone = myfield[11].css('p span::text').get().strip()
            fax = ''
            if myfield[12].css('p span::text').get() :
                fax = myfield[12].css('p span::text').get().strip()
            email = ''
            if myfield[13].css('p span::text').get() :
                email = myfield[13].css('p span::text').get().strip()
            tgl_update = '' 
            if myfield[14].css('p span::text').get() :
                tgl_update = myfield[14].css('p span::text').get().strip()
            myyield = {
                'kode':re.sub(r'[^\x00-\x7F]+',' ', kode),
                'tgl_registrasi':re.sub(r'[^\x00-\x7F]+',' ', tgl_registrasi),
                'nama':re.sub(r'[^\x00-\x7F]+',' ', nama),
                'jenis':re.sub(r'[^\x00-\x7F]+',' ', jenis),
                'kelas':re.sub(r'[^\x00-\x7F]+',' ', kelas),
                'direktur':re.sub(r'[^\x00-\x7F]+',' ', direktur),
                'pemilik':re.sub(r'[^\x00-\x7F]+',' ', pemilik),
                'alamat':re.sub(r'[^\x00-\x7F]+',' ', alamat),
                'kab_kota':re.sub(r'[^\x00-\x7F]+',' ', kab_kota),
                'kodepos':re.sub(r'[^\x00-\x7F]+',' ', kodepos),
                'telephone':re.sub(r'[^\x00-\x7F]+',' ', telephone),
                'fax':re.sub(r'[^\x00-\x7F]+',' ', fax),
                'email':re.sub(r'[^\x00-\x7F]+',' ', email),
                'tgl_update':re.sub(r'[^\x00-\x7F]+',' ', tgl_update)
            }
            with open(self.filename1, 'a') as f:
                w = csv.DictWriter(f, self.fieldnames, lineterminator='\n', delimiter='|')
                if iterasi == 1 :
                    w.writeheader()
                w.writerow(myyield)
            iterasi += 1

    def parse_detail(self, response) :
        i=0
        myyield = {
                'update':''
            }
        if response.css('table[id=fields_block1] tr td::text') :
            for row in response.css('table[id=fields_block1] tr'):
                data = row.css('td')
                if i>=1 and i <9 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")] = data[1].css('::text').get().strip().replace("\n", "").replace('\r', ' ')
                if i>=10 and i <18 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_").replace("/", "_")]= data[1].css('::text').get().strip().replace("\n", "").replace('\r', ' ')
                if i>=19 and i <27 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().strip().replace("\n", "").replace('\r', ' ')
                if i>=28 and i <31 :
                    myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().strip().replace("\n", "").replace('\r', ' ')
                i += 1 
            #self.fieldnames2.extend(myyield.keys())
            with open(self.filename2, 'a') as f:
                w = csv.DictWriter(f, self.fieldnames2, lineterminator='\n', delimiter='|')
                w.writerow(myyield)
            