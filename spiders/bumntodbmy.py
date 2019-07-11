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
    #fieldnames = ['nomor','bumn', 'logo', 'sektor','situs']

    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        for row in reader :
            rowid = get_one(row[1])
            if rowid :
                rowid = update(rowid,website,timestr,row[1],row[3],row[4])
            else :
                rowid = insert(website,timestr,row[1],row[3],row[4])
                print("saved data %s with id %s", (rowid,row[1]))
    os.remove(filecsv)

def insert(website,crdate,nama,sektor,situs):
    mysqldb = mysql.connector
    """create table bumn(
        id int not null auto_increment primary key, 
        nama varchar(200) not null, 
        sektor varchar(200) null, 
        situs varchar(200) null)
        """
    """ insert a new vendor into the vendors table """
    """ sql = INSERT INTO websites(name,crdate,data1)
             VALUES(%s,%s,%s) ;"""
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO bumn (nama, sektor, situs, crawl_date) 
    VALUES (%s, %s, %s,%s);"""
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
        cur.execute(sql, (nama,sektor,situs,crdate))
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

def update(id,website,crdate,nama,sektor,situs):
    mysqldb = mysql.connector
    """create table bumn(
        id int not null auto_increment primary key, 
        nama varchar(200) not null, 
        sektor varchar(200) null, 
        situs varchar(200) null)
        """
    """ insert a new vendor into the vendors table """
    """ sql = INSERT INTO websites(name,crdate,data1)
             VALUES(%s,%s,%s) ;"""
    """ Insert with cek if exists"""
    sql = """ 
    UPDATE bumn 
    SET nama=%s, sektor=%s,situs=%s, crawl_date=%s
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
        cur.execute(sql, (nama,sektor,situs,crdate,id))
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
        sql ="SELECT id FROM bumn WHERE nama = %s LIMIT 1"
        cur.execute(sql,(name,))
        rowid = cur.fetchone()
        if rowid :
            row = rowid[0]
        #print(rowid)
        cur.close()
    except (Exception, mysqldb.DatabaseError) as error:
        print(error)
        print('get one error')
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

#if __name__ == '__main__':
#    get_one('PT Semen Baturaja')