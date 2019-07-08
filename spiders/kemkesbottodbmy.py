#! python2
#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
from configdbmy import config
import csv
import time

def readcsvandupdate(website,filecsv):
    print("reading file from %s", filecsv)
    timestr = time.strftime("%Y-%m-%d %H:%M:%S")
    #fieldnames = ['kode','tgl_registrasi','nama','jenis','kelas','direktur','alamat','penyelenggara','kab_kota','kodepos','telephone','fax','tgl_update','produk']
    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        for row in reader :
            rowid = get_one(row[1])
            if rowid :
                rowid = update(rowid,website,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13])
            else :
                rowid = insert(website,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13])
            print("saved data %s with id %s", (rowid,row[1]))
        print("save file one by one")

def insert(website,crdate,kode,tgl_registrasi,nama,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk):
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
        cur.execute(sql, (crdate, kode,tgl_registrasi,nama,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk))
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

def update(id,website,crdate,kode,tgl_registrasi,nama,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk):
    mysqldb = mysql.connector
    """ Insert with cek if exists"""
    sql = """ 
    UPDATE kemkes_header 
    SET crawl_date=%s, kode=%s,tgl_registrasi=%s,nama=%s,jenis=%s,kelas=%s,direktur=%s,alamat=%s,penyelenggara=%s,kab_kota=%s,kodepos=%s,telephone=%s,fax=%s,tgl_update=%s,produk=%s
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
        cur.execute(sql, (crdate,kode,tgl_registrasi,nama,jenis,kelas,direktur,alamat,penyelenggara,kab_kota,kodepos,telephone,fax,tgl_update,produk,id))
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
        cur.execute("SELECT id FROM kemkes_header WHERE nama = %s", name)
        #print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()
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