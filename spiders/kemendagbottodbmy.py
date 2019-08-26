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
    #fieldnames = ['no', 'nama_perusahaan', 'kontak', 'posisi', 'alamat', 'ph', 'fax','kecamatan','kabupaten','provinsi','product']
    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        next(reader)
        for row in reader :
            rowid = get_one(row[1])
            if rowid :
                rowid = update(rowid,website,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10])
            else :
                rowid = insert(website,timestr,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10])
    os.remove(filecsv)

def insert(website,crdate,no, nama_perusahaan, kontak, posisi, alamat, ph, fax,kecamatan,kabupaten,provinsi,product):
    mysqldb = mysql.connector
    #fieldnames = ['no', 'nama_perusahaan', 'kontak', 'posisi', 'alamat', 'ph', 'fax','kecamatan','kabupaten','provinsi','product']
    """create table kemendag(
        id int not null auto_increment primary key, 
        no varchar(100)  null, 
        nama_perusahaan varchar(200) null, 
        kontak varchar(200) null,
        posisi varchar(200)  null, 
        alamat varchar(200)  null, 
        ph varchar(50)  null,
        fax varchar(50)  null,
        kecamatan varchar(200)  null,
        kabupaten varchar(200)  null,
        provinsi varchar(200)  null,
        product varchar(200)  null,
        crawl_date datetime null
        )
        """
    """ insert a new vendor into the vendors table """
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO kemendag (crawl_date, no, nama_perusahaan, kontak, posisi, alamat, ph, fax,kecamatan,kabupaten,provinsi,product) 
    VALUES (%s,%s, %s, %s,%s,%s, %s, %s,%s,%s, %s, %s);"""
    conn = None
    vendor_id = None
    try:
        # read database configuration
        params = config()
        # connect to the MySQL database
        conn = mysqldb.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (crdate, no, nama_perusahaan, kontak, posisi, alamat, ph, fax,kecamatan,kabupaten,provinsi,product))
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

def update(id,website,crdate,no, nama_perusahaan, kontak, posisi, alamat, ph, fax,kecamatan,kabupaten,provinsi,product):
    mysqldb = mysql.connector
    """ Insert with cek if exists"""
    sql = """ 
    UPDATE kemendag 
    SET crawl_date=%s, no=%s, nama_perusahaan=%s, kontak=%s, posisi=%s, alamat=%s, ph=%s, fax=%s,kecamatan=%s,kabupaten=%s,provinsi=%s,product=%s
    WHERE id=%s
    ;"""
    conn = None
    vendor_id = None
    try:
        # read database configuration
        params = config()
        # connect to the MySQL database
        conn = mysqldb.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (crdate,no, nama_perusahaan, kontak, posisi, alamat, ph, fax,kecamatan,kabupaten,provinsi,product,id))
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
        sql = "SELECT id FROM kemendag WHERE nama_perusahaan = %s limit 1"
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
    """ Connect to the MySQL database server """
    mysqldb = mysql.connector
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the MySQL server
        print('Connecting to the Mysql database...')
        conn = mysqldb.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
   # execute a statement
        print('Mysql database version:')
        cur.execute('SELECT version()')
 
        # display the MySQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the MySQL
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')