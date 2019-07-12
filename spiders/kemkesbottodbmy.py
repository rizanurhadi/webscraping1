#! python2
#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
from configdbmy import config
import csv
import time
import os

def readcsvandupdate(website,filecsv):
    print("reading file from %s", filecsv)
    timestr = time.strftime("%Y-%m-%d %H:%M:%S")
    #fieldnames = ['kode_rs','rumah_sakit','tgl_registrasi','jenis','kls_rs','direktur_rs','latar_belakang_pendidikan','bidan_pendidik','dokter_gigi_sp_gigi_tiruan','pekarya','alatrs_Meja_Operasi','dokter_sp_bedah_plastik','kesja','analis_farmasi','tempat_tidur_tt_di_ruang_isolasi','humas','perawat_maternitas','alatrs_CT_Scan','berlaku_sampai_dengan','rumah_sakit','entomologi','dietisien','biostatistik','informasi_kesehatan','dokter_sp_lainnya','widyaiswara','luas_bangunan','akupunturis','sanitasi','tempat_tidur_igd','alatrs_E_E_G','fax','tempat_tidur_hcu','jenis','dokter_sub_spesialis','fisioterapi','lainya_SIMRS','administrasi_keuangan','dokter_sp_bedah_orthopedi','indikator_thn_sblm_T_O_I','status_penyelenggara','dokter_gigi_sp_periodonsia','tempat_tidur_nicu','dokter_sp_psikiatri','dokter_sp_bedah_saraf','dokter_sp_orthopedi','dokter_gigi_sp_konservasi','tanggal_surat_ijin','dokter_sp_og','kode_rs','perawat_komunitas','alatrs_E_K_G','tempat_tidur_vip','dokter_gigi_sp_radiologi','informasi_teknologi','surat_ijin_dari','sifat_surat_ijin','alatrs_Defibrilator','indikator_thn_sblm_N_D_R','no_surat_ijin','kesehatan_lingkungan','dokter_sp_tht','kab_kota','tempat_tidur_kelas_iii','dokter_sp_jp','perawat_bedah','alatrs_Inkubator','dokter_gigi_sp_bedah_mulut','dokter_sp_paru','email','nutrisionis','rekam_medik','epidemiologi','dokter_sp_patologi_anatomi','latar_belakang_pendidikan','indikator_thn_sblm_Rawat_Jalan','dokter_sp_saraf','refraksionis','dokter_sp_rm','analis_kesehatan','dokter_sp_bedah_anak','dokter_sp_ofthalmologi','teknisi_gigi','apoteker','radiografer','dokter_sp_a','dokter_sp_forensik','dokter_gigi_sp_karang_gigi','tempat_tidur_iccu','dokter_gigi_sp_penyakit_mulut','dokter_sp_m','tempat_tidur_kelas_ii','terapi_wicara','tenaga_non_kes','indikator_thn_sblm_A_L_O_S','teknisi_transfusi_darah','perilaku','dokter_sp_kulit_dan_kelamin','masa_berlaku_surat_ijin','alatrs_U_S_G','perpustakaan','alatrs_Ventilator','direktur_rs','indikator_thn_sblm_I_G_D','lainya_Ambulan','tempat_tidur_icu','promosi_kesehatan','dokter_sp_urologi','dokter_gigi','alatrs_Mesin_Anestesi','alatrs_X-Ray','dokter_sp_bedah_thoraks','dosen','elektromedis','website','telepon','tempat_tidur_tt_bayi_baru_lahir','status_akreditasi','lainya_Bank_Darah','mikrobiologi','dokter_sp_pd','dokter_sp_rad','perawat_lainnya','dokter_sp_pk','alatrs_Autoclav','dr_umum','dokter_gigi_sp_lainnya','perawat_anak','pemilik','indikator_thn_sblm_Rawat_Inap','indikator_thn_sblm_G_D_R','teknisi_kardiovaskular','perawat_gigi','tgl_akreditas','kls_rs','perawat_anestesi','ners','luas_tanah','administrasi_kesehatan','alatrs_Blue_Light','alatrs_M_R_I','hukum','alamat','ortotik','dokter_gigi_sp_anak','jaminan_kesehatan','kesmas_lainnya','tgl_registrasi','telepon_humas','dokter_sp_b','tempat_tidur_picu','indikator_thn_sblm_B_O_R','dokter_sp_okupasi','tempat_tidur_tt_di_ruang_operasi','kode_pos','update','lainya_Layanan_Unggulan','dokter_sp_an','perencanaan','tempat_tidur_vvip','terapi_okupasi','dokter_sp_kes._jiwa','radioterapis','psikologi','program_kesehatan','bidan_klinik','reproduksi','pelaporan','tempat_tidur_kelas_i']
    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        next(reader)
        for row in reader :
            rowid = get_one(row[1])
            if rowid :
                update(rowid,website,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13])
            else :
                rowid = insert(website,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13])
            print("saved data %s with id %s", (rowid,row[1]))
        #print("save file one by one")
    os.remove(filecsv)

def insert(website,crdate,kode,nama,tgl_registrasi,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk):
    mysqldb = mysql.connector
    #fieldnames = ['kode','tgl_registrasi','nama','jenis','kelas','direktur','alamat','penyelenggara','kab_kota','kodepos','telephone','fax','tgl_update','produk']
    """create table kemkes_header(
        id int not null auto_increment primary key, 
        kode varchar(100)  null, 
        nama varchar(200) null, 
        tgl_registrasi varchar(50) null,
        jenis varchar(100)  null, 
        kelas varchar(100)  null, 
        direktur varchar(200)  null,
        alamat varchar(200)  null,
        penyelenggara varchar(200)  null,
        kab_kota varchar(200)  null,
        kodepos varchar(200)  null,
        telephone varchar(50)  null,
        fax varchar(50)  null,
        tgl_update varchar(50)  null,
        produk varchar(200)  null,
        crawl_date datetime null
        )
        """
    """ insert a new vendor into the vendors table """
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO kemkes_header (crawl_date, kode,tgl_registrasi,nama,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk) 
    VALUES (%s,%s, %s, %s,%s,%s, %s, %s,%s,%s, %s, %s, %s, %s, %s);"""
    conn = None
    vendor_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = mysqldb.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (crdate, kode,nama,tgl_registrasi,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk))
        # get the generated id back
        #vendor_id = cur.fetchone()[0]
        vendor_id = cur.lastrowid
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
    return vendor_id

def update(id,website,crdate,kode,nama,tgl_registrasi,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk):
    mysqldb = mysql.connector
    """ Insert with cek if exists"""
    sql = """ 
    UPDATE kemkes_header 
    SET crawl_date=%s, kode=%s,nama=%s,tgl_registrasi=%s,jenis=%s,kelas=%s,direktur=%s,alamat=%s,penyelenggara=%s,kab_kota=%s,kodepos=%s,telephone=%s,fax=%s,tgl_update=%s,produk=%s
    WHERE id=%s
    ;"""
    conn = None
    vendor_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = mysqldb.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (crdate,kode,nama,tgl_registrasi,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk,id))
        # get the generated id back
        #vendor_id = cur.fetchone()[0]
        vendor_id = cur.lastrowid
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
    return vendor_id

def get_one(name):
    mysqldb = mysql.connector
    """ query data from the vendors table """
    conn = None
    row = None
    try:
        params = config()
        conn = mysqldb.connect(**params)
        cur = conn.cursor()
        sql = "SELECT id FROM kemkes_header WHERE nama = %s limit 1"
        cur.execute(sql,(name,))
        rowid = cur.fetchone()
        if rowid :
            row = rowid[0]
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return row

def connect():
    """ Connect to the PostgreSQL database server """
    mysqldb = mysql.connector
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the Mysql database...')
        conn = mysqldb.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
   # execute a statement
        print('Mysql database version:')
        cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')