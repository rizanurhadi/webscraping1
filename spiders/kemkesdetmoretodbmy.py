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
    #fieldnames = ['kode_rs','nama_field','nilai_field'])
    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        next(reader)
        for row in reader :
            rowid = get_one(row[0],row[1])
            if rowid :
                update(rowid,website,timestr,row[0],row[1],row[2])
            else :
                rowid = insert(website,timestr,row[0],row[1],row[2])
            print("saved data %s with id %s", (rowid,row[1]))
        #print("save file one by one")
    #os.remove(filecsv)

def insert(website,crdate,kode_rs,nama_field,nilai_field):
    mysqldb = mysql.connector

    """create table kemkes_profile_more(
        id int not null auto_increment primary key, 
        kode_rs varchar(100)  null, 
        nama_field varchar(200) null,
        nilai_field varchar(200) null,
        crawl_date datetime null
        )
        """
    """ insert a new vendor into the vendors table """
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO kemkes_profile_more (crawl_date, kode_rs,nama_field,nilai_field) 
    VALUES (%s,%s, %s, %s);"""
    
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
        cur.execute(sql, (crdate, kode_rs,nama_field,nilai_field))
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

def update(id,website,crdate,kode_rs,nama_field,nilai_field):
    mysqldb = mysql.connector
    """ Insert with cek if exists"""
    sql = """ 
    UPDATE kemkes_profile_more 
    SET crawl_date=%s, kode_rs=%s, nama_field=%s, nilai_field=%s
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
        cur.execute(sql, (crdate,kode_rs,nama_field,nilai_field,id))
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

def get_one(kode_rs,nama_field):
    mysqldb = mysql.connector
    """ query data from the vendors table """
    conn = None
    row = None
    try:
        params = config()
        conn = mysqldb.connect(**params)
        cur = conn.cursor()
        sql = "SELECT id FROM kemkes_profile_more WHERE kode_rs = %s AND nama_field=%s limit 1"
        cur.execute(sql,(kode_rs,nama_field,))
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