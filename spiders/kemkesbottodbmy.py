#! python2
#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
from .configdbmy import config
import csv
import time
import os

def readcsvandupdate(website,filecsv):
    print("reading file from %s", filecsv)
    timestr = time.strftime("%Y-%m-%d %H:%M:%S")
    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        next(reader)
        for row in reader :
            rowid = get_one(row[1])
            if rowid :
                update(rowid,website,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14])
            else :
                rowid = insert(website,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14])
            print("saved data %s with id %s", (rowid,row[1]))
        #print("save file one by one")
    #os.remove(filecsv)

def insert(website,crdate,kode,nama,tgl_registrasi,jenis,kelas,direktur,pemilik,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,email):
    mysqldb = mysql.connector
    #fieldnames = ['kode','tgl_registrasi','nama','jenis','kelas','direktur','pemilik','alamat','penyelenggara','kab_kota','kodepos','telephone','fax','tgl_update','email']
    """create table kemkes_header(
        id int not null auto_increment primary key, 
        kode varchar(100)  null, 
        nama varchar(200) null, 
        tgl_registrasi varchar(50) null,
        jenis varchar(100)  null, 
        kelas varchar(100)  null, 
        direktur varchar(200)  null,
        pemilik varchar(100)  null,
        alamat varchar(200)  null,
        penyelenggara varchar(200)  null,
        kab_kota varchar(200)  null,
        kodepos varchar(200)  null,
        telephone varchar(50)  null,
        fax varchar(50)  null,
        tgl_update varchar(50)  null,
        email varchar(200)  null,
        crawl_date datetime null
        )
        """
    """ insert a new vendor into the vendors table """
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO kemkes_header (crawl_date, kode,nama,tgl_registrasi,jenis,kelas,direktur,pemilik,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,email) 
    VALUES (%s,%s, %s, %s,%s,%s, %s, %s,%s,%s, %s, %s, %s, %s, %s,%s);"""
    #print(sql % (crdate, kode,nama,tgl_registrasi,jenis,kelas,direktur,pemilik,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,email))
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
        cur.execute(sql, (crdate, kode,nama,tgl_registrasi,jenis,kelas,direktur,pemilik,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,email))
        # get the generated id back
        #vendor_id = cur.fetchone()[0]
        vendor_id = cur.lastrowid
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print('insert error')
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
    return vendor_id

def update(id,website,crdate,kode,nama,tgl_registrasi,jenis,kelas,direktur,pemilik,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,email):
    mysqldb = mysql.connector
    """ Insert with cek if exists"""
    sql = """ 
    UPDATE kemkes_header 
    SET crawl_date=%s, kode=%s,nama=%s,tgl_registrasi=%s,jenis=%s,kelas=%s,direktur=%s,pemilik=%s,alamat=%s,penyelenggara=%s,kab_kota=%s,kodepos=%s,telephone=%s,fax=%s,tgl_update=%s,email=%s
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
        cur.execute(sql, (crdate,kode,nama,tgl_registrasi,jenis,kelas,direktur,pemilik,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,email,id))
        # get the generated id back
        #vendor_id = cur.fetchone()[0]
        vendor_id = cur.lastrowid
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print('update error')
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