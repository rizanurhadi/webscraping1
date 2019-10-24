#! python2
#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
from .configdbmy import config
import csv
import time
import os

def readcsvandupdate(url,filecsv):
    print("reading file from %s", filecsv)
    timestr = time.strftime("%Y-%m-%d %H:%M:%S")
    #fieldnames = ['kode_rs','rumah_sakit','tgl_registrasi','jenis','kls_rs','direktur_rs','latar_belakang_pendidikan','pemilik','alamat','kode_pos','telepon','fax','email','telepon_humas','website','status_akreditasi']
    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        next(reader)
        for row in reader :
            rowid = get_one(row[0])
            if rowid :
                update(rowid,url,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])
            else :
                rowid = insert(url,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15])
            print("saved data %s with id %s", (rowid,row[1]))
        #print("save file one by one")
    #os.remove(filecsv)

def insert(url,crdate,kode_rs,rumah_sakit,tgl_registrasi,jenis,kls_rs,direktur_rs,latar_belakang_pendidikan,pemilik,alamat,kode_pos,telepon,fax,email,telepon_humas,website,status_akreditasi):
    mysqldb = mysql.connector
    #fieldnames = ['kode_rs','tgl_registrasi','rumah_sakit','jenis','kls_rs','direktur_rs','latar_belakang_pendidikan']
    """create table kemkes_profile(
        id int not null auto_increment primary key, 
        kode_rs varchar(100)  null, 
        rumah_sakit varchar(200) null, 
        tgl_registrasi varchar(50) null,
        jenis varchar(100)  null, 
        kls_rs varchar(100)  null, 
        direktur_rs varchar(200)  null,
        latar_belakang_pendidikan varchar(200)  null,
        pemilik varchar(200) null,
        alamat varchar(200) null,
        kode_pos varchar(200) null,
        telepon varchar(200) null,
        fax varchar(200) null,
        email varchar(200) null,
        telepon_humas varchar(200) null,
        website varchar(200) null,
        status_akreditasi varchar(200) null,
        crawl_date datetime null
        )
        """
    """ insert a new vendor into the vendors table """
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO kemkes_profile (crawl_date, kode_rs,rumah_sakit,tgl_registrasi,jenis,kls_rs,direktur_rs,latar_belakang_pendidikan,pemilik,alamat,kode_pos,telepon,fax,email,telepon_humas,website,status_akreditasi) 
    VALUES (%s,%s,%s,%s,%s,%s, %s, %s,%s, %s,%s,%s,%s, %s, %s,%s, %s);"""
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
        cur.execute(sql, (crdate, kode_rs,rumah_sakit,tgl_registrasi,jenis,kls_rs,direktur_rs,latar_belakang_pendidikan,pemilik,alamat,kode_pos,telepon,fax,email,telepon_humas,website,status_akreditasi))
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

def update(id,url,crdate,kode_rs,rumah_sakit,tgl_registrasi,jenis,kls_rs,direktur_rs,latar_belakang_pendidikan,pemilik,alamat,kode_pos,telepon,fax,email,telepon_humas,website,status_akreditasi):
    mysqldb = mysql.connector
    """ Insert with cek if exists"""
    sql = """ 
    UPDATE kemkes_profile 
    SET crawl_date=%s, kode_rs=%s,rumah_sakit=%s,tgl_registrasi=%s,jenis=%s,kls_rs=%s,direktur_rs=%s,latar_belakang_pendidikan=%s,pemilik=%s,alamat=%s,kode_pos=%s,telepon=%s,fax=%s,email=%s,telepon_humas=%s,website=%s,status_akreditasi=%s
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
        cur.execute(sql, (crdate,kode_rs,rumah_sakit,tgl_registrasi,jenis,kls_rs,direktur_rs,latar_belakang_pendidikan,pemilik,alamat,kode_pos,telepon,fax,email,telepon_humas,website,status_akreditasi,id))
        # get the generated id back
        #vendor_id = cur.fetchone()[0]
        vendor_id = cur.lastrowid
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
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
        sql = "SELECT id FROM kemkes_profile WHERE kode_rs = %s limit 1"
        cur.execute(sql,(name,))
        rowid = cur.fetchone()
        if rowid :
            row = rowid[0]
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
        print('get error')
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