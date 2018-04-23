import quandl
import numpy as np
import configparser, os
import pymysql
import datetime
import time
import json

config = configparser.ConfigParser()
config.readfp(open('defaults.cfg'))
apikey = config.get('quandl', 'apikey')

quandl.ApiConfig.api_key = apikey
quandl.ApiConfig.api_version = '2015-04-09'

dbname = 'USTREASURY/YIELD'

def insert_data(startdate):
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
    con.cursor().execute("insert into fetchhistory values ('%s', NULL)" % (currenttime))
    con.commit()


def lastfetch():
    query = "select max(fetchtime) from fetchhistory"
    with con.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()[0][0]




con = pymysql.connect(host = config.get('database', 'hostname'),
                      user = config.get('database', 'username'),
                      passwd = config.get('database', 'password'),
                      db = config.get('database', 'database'))




query = "select `date`, `value` from quantldata ORDER BY id DESC LIMIT 1 where `columnname='10 YR'"
with con.cursor() as cursor:
    cursor.execute(query)
    date, value = cursor.fetchall()[0]
    #print(date)
    if date != datetime.date.today() and datetime.datetime.now() - lastfetch() > datetime.timedelta(hours = 1):
        insert_data(date + datetime.timedelta(days=1))

    body = json.dumps(value)
    print("Status: 200 OK")
    print("Content-Type: application/json")
    print("Length:", len(body))
    print("")
    print(body)
    
con.close()
