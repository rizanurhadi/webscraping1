#! python2
#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
from .configdbmy import config
import csv
import time
import os

def readcsvandupdate(url,filecsv):
    #READ FORLAP PT PROFILE
    print("reading file from %s", filecsv)
    timestr = time.strftime("%Y-%m-%d %H:%M:%S")
    #fieldnames = ['kode','status_pt','tanggal_sk_pt','tanggal_berdiri','perguruan_tinggi','website','telepon','kode_pos','email','faximile','kota_kabupaten','alamat','nomor_sk_pt','jml_mhs_1718','jml_dosen_tetap_1718','rasio_dosen_mhs_1718','jml_mhs_1819','jml_dosen_tetap_1819','rasio_dosen_mhs_1819']
    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        next(reader)
        for row in reader :
            rowid = get_one(row[0])
            if rowid :
                rowid = update(rowid,url,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18])
            else :
                rowid = insert(url,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18])
    os.remove(filecsv)

def insert(url,crdate,kode,status_pt,tanggal_sk_pt,tanggal_berdiri,perguruan_tinggi,website,telepon,kode_pos,email,faximile,kota_kabupaten,alamat,nomor_sk_pt,jml_mhs_1718,jml_dosen_tetap_1718,rasio_dosen_mhs_1718,jml_mhs_1819,jml_dosen_tetap_1819,rasio_dosen_mhs_1819):
    mysqldb = mysql.connector
    #fieldnames = ['kode','nama', 'link_detail', 'prov','kategori','status','jml_dosen_tetap_1718', 'jml_mhs_1718' , 'rasio_dosen_mhs_1718', 'jml_dosen_tetap_1819', 'jml_mhs_1819' , 'rasio_dosen_mhs_1819']
    """create table forlap_profile(
        id int not null auto_increment primary key, 
        kode varchar(50)  null, 
        status_pt varchar(50)  null, 
        tanggal_sk_pt varchar(50)  null, 
        tanggal_berdiri varchar(50)  null, 
        perguruan_tinggi varchar(200)  null, 
        website varchar(200)  null, 
        telepon varchar(50)  null, 
        kode_pos varchar(20)  null, 
        email varchar(200)  null, 
        faximile varchar(50)  null, 
        kota_kabupaten varchar(200)  null, 
        alamat varchar(200)  null, 
        nomor_sk_pt varchar(200)  null, 
        jml_mhs_1718 varchar(20)  null, 
        jml_dosen_tetap_1718 varchar(20)  null, 
        rasio_dosen_mhs_1718 varchar(20)  null, 
        jml_mhs_1819 varchar(20)  null, 
        jml_dosen_tetap_1819 varchar(20)  null, 
        rasio_dosen_mhs_1819  varchar(20)  null,
        crawl_date datetime null
        )
        """
    """ insert a new vendor into the vendors table """
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO forlap_profile (crawl_date, kode,status_pt,tanggal_sk_pt,tanggal_berdiri,perguruan_tinggi,website,telepon,kode_pos,email,faximile,kota_kabupaten,alamat,nomor_sk_pt,jml_mhs_1718,jml_dosen_tetap_1718,rasio_dosen_mhs_1718,jml_mhs_1819,jml_dosen_tetap_1819,rasio_dosen_mhs_1819) 
    VALUES (%s,%s, %s, %s,%s,%s, %s, %s,%s,%s, %s, %s,%s,%s, %s,%s,%s, %s, %s,%s);"""
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
        cur.execute(sql, (crdate, kode,status_pt,tanggal_sk_pt,tanggal_berdiri,perguruan_tinggi,website,telepon,kode_pos,email,faximile,kota_kabupaten,alamat,nomor_sk_pt,jml_mhs_1718,jml_dosen_tetap_1718,rasio_dosen_mhs_1718,jml_mhs_1819,jml_dosen_tetap_1819,rasio_dosen_mhs_1819))
        
        # get the generated id back
        #vendor_id = cur.fetchone()[0]
        vendor_id = cur.lastrowid
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
        print('insert error')
        
    finally:
        if conn is not None:
            conn.close()
 
    return vendor_id

def update(id,url,crdate,kode,status_pt,tanggal_sk_pt,tanggal_berdiri,perguruan_tinggi,website,telepon,kode_pos,email,faximile,kota_kabupaten,alamat,nomor_sk_pt,jml_mhs_1718,jml_dosen_tetap_1718,rasio_dosen_mhs_1718,jml_mhs_1819,jml_dosen_tetap_1819,rasio_dosen_mhs_1819):
    mysqldb = mysql.connector
    """ Insert with cek if exists"""
    sql = """ 
    UPDATE forlap_profile 
    SET crawl_date=%s, kode=%s, status_pt=%s, tanggal_sk_pt=%s, tanggal_berdiri=%s, perguruan_tinggi=%s, website=%s, telepon=%s, kode_pos=%s, email=%s, faximile=%s, kota_kabupaten=%s, alamat=%s, nomor_sk_pt=%s, jml_mhs_1718=%s, jml_dosen_tetap_1718=%s, rasio_dosen_mhs_1718=%s, jml_mhs_1819=%s, jml_dosen_tetap_1819=%s, rasio_dosen_mhs_1819 =%s
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
        cur.execute(sql, (crdate,kode,status_pt,tanggal_sk_pt,tanggal_berdiri,perguruan_tinggi,website,telepon,kode_pos,email,faximile,kota_kabupaten,alamat,nomor_sk_pt,jml_mhs_1718,jml_dosen_tetap_1718,rasio_dosen_mhs_1718,jml_mhs_1819,jml_dosen_tetap_1819,rasio_dosen_mhs_1819,id))
        # get the generated id back
        #vendor_id = cur.fetchone()[0]
        vendor_id = cur.lastrowid
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        print('update suskes')
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
        print('update error')
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
        sql = "SELECT id FROM forlap_profile WHERE kode = %s LIMIT 1"
        cur.execute(sql,(name,))
        #print("The number of parts: ", cur.rowcount)
        rowid = cur.fetchone()
        if rowid :
            row = rowid[0]
        cur.close()
        print('getone sukses')
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
        print('getone error')
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