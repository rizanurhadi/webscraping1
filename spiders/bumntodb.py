#! python2
#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
from configdb import config
import csv
import time

def readcsvandupdate(website,filecsv):
    print("reading file from %s", filecsv)
    timestr = time.strftime("%Y-%m-%d %H:%M:%S")
    #fieldnames = ['nomor','bumn', 'logo', 'sektor','situs']

    csv.register_dialect('myDialect', delimiter = '|')
    with open(filecsv, 'r') as f:
        reader = csv.reader(f, dialect='myDialect')
        for row in reader :
            insertid = insert(website,timestr,row[1])
            print("saved data %s with id %s", (insertid,row[1]))
        print("save file one by one")


def insert(name,crdate,data1):
    """ insert a new vendor into the vendors table """
    """ sql = INSERT INTO websites(name,crawled_date,data1)
             VALUES(%s,%s,%s) RETURNING id;
    """
    """create table bumn(
        id int not null auto_increment primary key, 
        nama varchar(200) not null, 
        sektor varchar(200) null, 
        situs varchar(200) null)
        """
    """ Insert with cek if exists"""
    sql = """ 
    INSERT INTO bumn(nama,crawled_date,data1)
    SELECT %s, %s, %s
    WHERE
        NOT EXISTS (
            SELECT id FROM websites WHERE data1 = %s
        ) RETURNING id;"""
    conn = None
    vendor_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (name,crdate,data1,data1))
        # get the generated id back
        vendor_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
    return vendor_id

def update(id, name, data1):
    """ update vendor name based on the vendor id """
    sql = """ UPDATE websites
                SET name = %s, data1 = %s
                WHERE id = %s"""
    conn = None
    updated_rows = 0
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the UPDATE  statement
        cur.execute(sql, (id, name,data1))
        # get the number of updated rows
        updated_rows = cur.rowcount
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
    return updated_rows

def testing_connection():
    connect()

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
   # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 
 
#if __name__ == '__main__':
#    testing_connection()