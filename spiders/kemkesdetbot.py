#! python2
# -*- coding: utf-8 -*-
import scrapy
import time
import csv
import os
from sys import exit
#import logging 
#from scrapy.utils.log import configure_logging  
from scrapy import signals
from . import kemkesdetbottodbmy


class KemkesdetbotSpider(scrapy.Spider):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    #configure_logging(install_root_handler = False) 
    #logging.basicConfig ( 
    #   filename = dir_path + '/../out/log_kemkesdet.txt', 
    #    format = '%(levelname)s: %(message)s', 
    #    level = logging.WARNING 
    #)
    timestr = time.strftime("%Y%m%d-%H%M%S")
    name = 'kemkesdetbot'
    allowed_domains = ['sirs.yankes.kemkes.go.id']
    start_urls = ['http://sirs.yankes.kemkes.go.id/rsonline/data_view.php?editid1=']
    fieldnames = ['kode_rs','rumah_sakit','tgl_registrasi','jenis','kls_rs','direktur_rs','latar_belakang_pendidikan','pemilik','alamat','kode_pos','telepon','fax','email','telepon_humas','website','status_akreditasi']
    #,'bidan_pendidik','dokter_gigi_sp_gigi_tiruan','pekarya','alatrs_Meja_Operasi','dokter_sp_bedah_plastik','kesja','analis_farmasi','tempat_tidur_tt_di_ruang_isolasi','humas','perawat_maternitas','alatrs_CT_Scan','berlaku_sampai_dengan','rumah_sakit','entomologi','dietisien','biostatistik','informasi_kesehatan','dokter_sp_lainnya','widyaiswara','luas_bangunan','akupunturis','sanitasi','tempat_tidur_igd','alatrs_E_E_G','tempat_tidur_hcu','jenis','dokter_sub_spesialis','fisioterapi','lainya_SIMRS','administrasi_keuangan'
    #,'dokter_sp_bedah_orthopedi','indikator_thn_sblm_T_O_I','status_penyelenggara','dokter_gigi_sp_periodonsia','tempat_tidur_nicu','dokter_sp_psikiatri','dokter_sp_bedah_saraf','dokter_sp_orthopedi','dokter_gigi_sp_konservasi','tanggal_surat_ijin','dokter_sp_og','kode_rs','perawat_komunitas','alatrs_E_K_G','tempat_tidur_vip','dokter_gigi_sp_radiologi','informasi_teknologi','surat_ijin_dari','sifat_surat_ijin','alatrs_Defibrilator','indikator_thn_sblm_N_D_R','no_surat_ijin','kesehatan_lingkungan','dokter_sp_tht','kab_kota','tempat_tidur_kelas_iii'
    #,'dokter_sp_jp','perawat_bedah','alatrs_Inkubator','dokter_gigi_sp_bedah_mulut','dokter_sp_paru','nutrisionis','rekam_medik','epidemiologi','dokter_sp_patologi_anatomi','latar_belakang_pendidikan','indikator_thn_sblm_Rawat_Jalan','dokter_sp_saraf','refraksionis','dokter_sp_rm','analis_kesehatan','dokter_sp_bedah_anak','dokter_sp_ofthalmologi','teknisi_gigi','apoteker','radiografer','dokter_sp_a','dokter_sp_forensik','dokter_gigi_sp_karang_gigi','tempat_tidur_iccu','dokter_gigi_sp_penyakit_mulut','dokter_sp_m','tempat_tidur_kelas_ii','terapi_wicara'
    #,'tenaga_non_kes','indikator_thn_sblm_A_L_O_S','teknisi_transfusi_darah','perilaku','dokter_sp_kulit_dan_kelamin','masa_berlaku_surat_ijin','alatrs_U_S_G','perpustakaan','alatrs_Ventilator','direktur_rs','indikator_thn_sblm_I_G_D','lainya_Ambulan','tempat_tidur_icu','promosi_kesehatan','dokter_sp_urologi','dokter_gigi','alatrs_Mesin_Anestesi','alatrs_X-Ray','dokter_sp_bedah_thoraks','dosen','elektromedis','tempat_tidur_tt_bayi_baru_lahir','lainya_Bank_Darah','mikrobiologi','dokter_sp_pd','dokter_sp_rad','perawat_lainnya','dokter_sp_pk','alatrs_Autoclav'
    #,'dr_umum','dokter_gigi_sp_lainnya','perawat_anak','indikator_thn_sblm_Rawat_Inap','indikator_thn_sblm_G_D_R','teknisi_kardiovaskular','perawat_gigi','tgl_akreditas','kls_rs','perawat_anestesi','ners','luas_tanah','administrasi_kesehatan','alatrs_Blue_Light','alatrs_M_R_I','hukum','ortotik','dokter_gigi_sp_anak','jaminan_kesehatan','kesmas_lainnya','tgl_registrasi','dokter_sp_b','tempat_tidur_picu','indikator_thn_sblm_B_O_R','dokter_sp_okupasi','tempat_tidur_tt_di_ruang_operasi','update','lainya_Layanan_Unggulan','dokter_sp_an','perencanaan','tempat_tidur_vvip'
    #,'terapi_okupasi','dokter_sp_kes._jiwa','radioterapis','psikologi','program_kesehatan','bidan_klinik','reproduksi','pelaporan','tempat_tidur_kelas_i'
    filename1 = dir_path + '/../out/kemses_detail_%s.csv' % timestr

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KemkesdetbotSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider):
        spider.logger.info('Signal sent then Spider closed. file out is : %s', self.filename1)
        #save to db here
        kemkesdetbottodbmy.readcsvandupdate(self.allowed_domains[0],self.filename1)
    
    def start_requests(self):
        iterasi = 1
        yield scrapy.Request(self.start_urls[0] + '1',meta={'iterasi':iterasi,'page':1})

    def parse(self, response):
        #print(len(response.css('table[id=fields_block1] tr td::text')[1].get())
        if response.status == 200 :
            i=0
            #myyield = RumahSakitDet()
            myyield = {
                'update':''
            }
            
            with open(self.filename1, 'a') as f:
                iterasi = response.meta.get('iterasi')
                if response.css('table[id=fields_block1] tr td::text') :
                    for row in response.css('table[id=fields_block1] tr'):
                        data = row.css('td')
                        if i>=1 and i <9 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")] = data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        if i>=10 and i <18 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_").replace("/", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        if i>=19 and i <27 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        if i>=28 and i <31 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        if i >= 32 and i < 46 :
                            myyield['tempat_tidur_' + data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        #Dokter
                        if i >= 48 and i < 63 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                            myyield[data[2].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[3].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        #DokterGIgi
                        if i >= 64 and i < 69 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                            myyield[data[2].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[3].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        #perawat
                        if i >= 70 and i < 74 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                            myyield[data[2].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[3].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        #bidan
                        if i >= 75 and i < 77 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                            myyield[data[2].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[3].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        #Keteknisian Medis
                        if i >= 78 and i < 88 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        if i >= 78 and i < 87 :
                            myyield[data[2].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[3].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        #Tenaga Kesehatan Lainnya
                        if i >= 89 and i < 94 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                            myyield[data[2].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[3].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        #Tenaga Non Kesehatan
                        if i >= 95 and i < 102 :
                            myyield[data[0].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[1].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                            myyield[data[2].css('::text').get().lower().replace(" ", "_").replace(".", "_").replace("-", "_")]= data[3].css('::text').get().encode('utf-8').strip().replace("\n", "").replace('\r', ' ')
                        #Tenaga Non Kesehatan
                        if i >= 103 and i < 116 :
                            myyield['alatrs_'+ data[0].css('::text').get().split(':', 1)[0].strip().replace("\n", "").replace('\r', ' ').replace(" ", "_")]= data[0].css('::text').get().encode('utf-8').split(':')[1].strip().replace("\n", "").replace('\r', ' ')
                            #yield { 'test' : 'alatrs_'+ data[0].css('::text').get().split(':', 1)[0].strip().replace("\n", "").replace('\r', ' ').replace(" ", "_") + '= scrapy.Field()' }
                        if i >= 103 and i < 111 :
                            myyield['indikator_thn_sblm_'+ data[1].css('::text').get().split(':', 1)[0].strip().replace("\n", "").replace('\r', ' ').replace(" ", "_")]= data[1].css('::text').get().encode('utf-8').split(':')[1].strip().replace("\n", "").replace('\r', ' ')
                            #yield { 'test' : 'indikator_thn_sblm_'+ data[1].css('::text').get().split(':', 1)[0].strip().replace("\n", "").replace('\r', ' ').replace(" ", "_") + '= scrapy.Field()' }
                        if i >= 103 and i < 107 :
                            myyield['lainya_'+ data[2].css('::text').get().split(':', 1)[0].strip().replace("\n", "").replace('\r', ' ').replace(" ", "_")]= data[2].css('::text').get().encode('utf-8').split(':')[1].strip().replace("\n", "").replace('\r', ' ')
                            #yield { 'test' : 'lainya_'+ data[2].css('::text').get().split(':', 1)[0].strip().replace("\n", "").replace('\r', ' ').replace(" ", "_") + '= scrapy.Field()' }
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
                exit(0) if page == 5 else None
                yield scrapy.Request(next_page, callback=self.parse, meta={'iterasi':iterasi,'page': page })