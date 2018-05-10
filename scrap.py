#!C:/Users/LyFei/AppData/Local/Programs/Python/Python36/Python

import quandl
import numpy as np
import configparser, os
import pymysql
import datetime
import time
import json
import sys

config = configparser.ConfigParser()
config.readfp(open('defaults.cfg'))
apikey = config.get('quandl', 'apikey')

quandl.ApiConfig.api_key = apikey
quandl.ApiConfig.api_version = '2015-04-09'

dbname = 'USTREASURY/YIELD'

def insert_data(dbname, startdate):

    db = quandl.Dataset(dbname)
    colnames = db.column_names

    ret = quandl.get(dbname, returns="numpy", start_date=startdate, end_date=datetime.date.today())

    if(len(ret) > 0):
        query = "INSERT INTO quantldata (`date`, `code`, `columnname`, `value`) VALUES (%s, %s, %s, %s)"

        cols = len(colnames)
        totquery = ""

        data = []

        for rows in ret:

            t = rows[0]
            for i in range(1, cols):
                val = float(rows[i]) if np.isnan(rows[i]) == False else -1
                data.append((t, dbname, colnames[i], val))
        with con.cursor() as cursor:
            cursor.executemany(query, data)
        con.commit()
    currenttime = time.strftime('%Y-%m-%d %H:%M:%S')
    con.cursor().execute("insert into fetchhistory values ('%s', '%s')" % (currenttime, dbname))
    con.commit()


def lastfetch(dbname):
    query = "select max(fetchtime) from fetchhistory where `code` ='%s'" % dbname
    with con.cursor() as cursor:
        cursor.execute(query)
        date_last = cursor.fetchall()[0][0]
        if date_last is None:
            date_last = datetime.datetime(1900, 1, 1)
        return date_last


data = sys.stdin.read()
js = json.loads(data)
dbname = js['dbname']
columnname = js['col']


con = pymysql.connect(host = config.get('database', 'hostname'),
                      user = config.get('database', 'username'),
                      passwd = config.get('database', 'password'),
                      db = config.get('database', 'database'))


query_last_date = "select max(`date`) from quantldata where `code`= '%s'" % dbname;

query_data = "select `date`, `value` from quantldata  where `columnname`='%s' and `code`='%s' ORDER BY id DESC LIMIT 1" % (columnname, dbname)
with con.cursor() as cursor:
    cursor.execute(query_last_date)
    date = cursor.fetchall()[0][0]
    if date is None:
        date = datetime.date(1900, 1, 1)
    
    if date != datetime.date.today() and datetime.datetime.now() - lastfetch(dbname) > datetime.timedelta(hours = 1):
        insert_data(dbname, date + datetime.timedelta(days=1))

    cursor.execute(query_data)
    

    date, value = cursor.fetchall()[0]

    data = {'value' : value, 'date' : date.strftime('%Y-%m-%d'), 'last_update_time' : lastfetch(dbname).strftime('%Y-%m-%d %H:%M:%S')}
    
    body = json.dumps(data)
    print("Status: 200 OK")
    print("Content-Type: application/json")
    print("Length:", len(body))
    print("")
    print(body)
con.close()
