#!/usr/bin/python
# -*- coding: UTF-8 -*-
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import datetime
from urllib import parse
import pymysql
import pandas as pd
import json
from ck_util import clickhouse_util

php_system_config = {
    'host': '10.17.15.98',
    'user': 'db_app',
    'password': 'Vee8lie3aiNee9sa',
    'database': 'new_ssp',
    'port': 3306
}

con = pymysql.connect(**php_system_config)


def execute_sql( day, client):
    sql_str = """
                SELECT fenix_offerid  as offer_id
, count(1) click
from real_fenix_click
where substring(time_slot ,1,8) = '{day}' and click_through  = '1' and slot = '62858868'
group by fenix_offerid

    """.format(day=day)

    result = client.execute(sql_str)
    df = pd.DataFrame(list(result), columns=[ 'offer_id', 'click'])
    return df


if __name__ == '__main__':
    from sys import argv

    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()
 
    date = datetime.datetime.now().strftime('%Y%m%d')
    print(date)
    df = execute_sql(date, client)

    cur = con.cursor()        
    result_dict={}
    for index,row in df.iterrows():
        key = str(row[0]) + "_" + "62858868"
        value = row[1]
        udata_sql = "insert into fenix_click_api(`key`,`value`) values ('%s','%s') ON DUPLICATE KEY UPDATE `value`= '%s' " % (key,value,value)
        print(udata_sql)
        cur.execute(udata_sql)
        con.commit()