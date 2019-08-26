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
    #fieldnames = ['id_ai','prov','nama_mall','detail_link']
    #fieldnames_detail = ['nama_mall','alamat','lokasi','pemilik','pengembang','pengurus','tanggal_dibuka','jumlah_toko_dan_jasa','jumlah_toko_induk','total_luas_pertokoan','jumlah_lantai','parkir','situs_web','kantor','didirikan','industri','akses_transportasi_umum','pendapatan','arsitek']
    #fieldnames_detail = ['nama_mall','kabkot','prov','alamat','lokasi','pemilik','pengembang','pengurus','tanggal_dibuka','jumlah_toko_dan_jasa','jumlah_toko_induk','total_luas_pertokoan','jumlah_lantai','parkir','situs_web','kantor','didirikan','industri','akses_transportasi_umum','pendapatan','arsitek']
    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        for row in reader :
            if len(row) > 5 :
                rowid = get_one(row[0])
                if rowid :
                    update(rowid,website,timestr,row[0],"NULL","NULL",row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18])
                    print("update2 data %s with id %s", (rowid,row[0]))
                else :
                    rowid = insert(website,timestr,row[0],"NULL","NULL",row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18])
                    print("saved2 data %s with id %s", (rowid,row[0]))
            else :
                rowid = get_one(row[3])
                if rowid :
                    update(rowid,website,timestr,row[3],row[2],row[1],"NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL")
                    print("update data %s with id %s", (rowid,row[0]))
                else :
                    rowid = insert(website,timestr,row[3],row[2],row[1],"NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL")
                    print("saved data %s with id %s", (rowid,row[3]))
    os.remove(filecsv)

def insert(website,crdate,nama_mall , kabkot , prov , alamat , lokasi , pemilik , pengembang , pengurus , tanggal_dibuka , jumlah_toko_dan_jasa , jumlah_toko_induk , total_luas_pertokoan , jumlah_lantai , parkir , situs_web , kantor , didirikan , industri , akses_transportasi_umum , pendapatan , arsitek ):
    mysqldb = mysql.connector
    """create table pusat_belanja(
        id int not null auto_increment primary key, 
        nama varchar(200) not null, 
        kabkot varchar(200) null, 
        prov varchar(200) null,
        alamat varchar(200) null,
        lokasi varchar(200) null,
        pemilik varchar(200) null,
        pengembang varchar(200) null,
        pengurus varchar(200) null,
        tanggal_dibuka varchar(200) null,
        jumlah_toko_dan_jasa varchar(200) null,
        jumlah_toko_induk varchar(200) null,
        total_luas_pertokoan varchar(200) null,
        jumlah_lantai varchar(200) null,
        parkir varchar(200) null,
        situs_web varchar(200) null,
        kantor varchar(200) null,
        didirikan varchar(200) null,
        industri varchar(200) null,
        akses_transportasi_umum varchar(200) null,
        pendapatan varchar(200) null,
        arsitek varchar(200) null,
        crawl_date datetime null
        )
        """
    """ insert a new vendor into the vendors table """
    """ sql = INSERT INTO websites(name,crdate,data1)
             VALUES(%s,%s,%s) ;"""
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO pusat_belanja (nama , kabkot , prov , alamat , lokasi , pemilik , pengembang , pengurus , tanggal_dibuka , jumlah_toko_dan_jasa , jumlah_toko_induk , total_luas_pertokoan , jumlah_lantai , parkir , situs_web , kantor , didirikan , industri , akses_transportasi_umum , pendapatan , arsitek , crawl_date) 
    VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s);"""
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
        cur.execute(sql, (nama_mall , kabkot , prov , alamat , lokasi , pemilik , pengembang , pengurus , tanggal_dibuka , jumlah_toko_dan_jasa , jumlah_toko_induk , total_luas_pertokoan , jumlah_lantai , parkir , situs_web , kantor , didirikan , industri , akses_transportasi_umum , pendapatan , arsitek,crdate))
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

def update(id,website,crdate,nama_mall , kabkot , prov , alamat , lokasi , pemilik , pengembang , pengurus , tanggal_dibuka , jumlah_toko_dan_jasa , jumlah_toko_induk , total_luas_pertokoan , jumlah_lantai , parkir , situs_web , kantor , didirikan , industri , akses_transportasi_umum , pendapatan , arsitek):
    mysqldb = mysql.connector
    sql = """ 
    UPDATE pusat_belanja 
    SET nama=%s, kabkot =%s, prov =%s, alamat =%s, lokasi =%s, pemilik =%s, pengembang =%s, pengurus =%s, tanggal_dibuka =%s, jumlah_toko_dan_jasa =%s, jumlah_toko_induk =%s, total_luas_pertokoan =%s, jumlah_lantai =%s, parkir =%s, situs_web =%s, kantor =%s, didirikan =%s, industri =%s, akses_transportasi_umum =%s, pendapatan =%s, arsitek =%s, crawl_date=%s
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
        cur.execute(sql, (nama_mall , kabkot , prov , alamat , lokasi , pemilik , pengembang , pengurus , tanggal_dibuka , jumlah_toko_dan_jasa , jumlah_toko_induk , total_luas_pertokoan , jumlah_lantai , parkir , situs_web , kantor , didirikan , industri , akses_transportasi_umum , pendapatan , arsitek,crdate,id))
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
        sql ="SELECT id FROM pusat_belanja WHERE nama = %s LIMIT 1"
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
#    dir_path = os.path.dirname(os.path.realpath(__file__))
#   readcsvandupdate('test',dir_path + '/../out/wikimall_detail_20190711-163607.csv')