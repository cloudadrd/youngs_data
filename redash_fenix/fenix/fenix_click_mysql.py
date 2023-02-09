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
    'host': 'php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com',
    'user': 'db_app',
    'password': 'Vee8lie3aiNee9sa',
    'database': 'new_ssp',
    'port': 3306
}

con = pymysql.connect(**php_system_config)


def execute_sql( day, client):
    sql_str = """
                SELECT fenix_offerid  as offer_id, slot
, count(1) click
from real_fenix_click
where substring(time_slot ,1,8) = '{day}' and click_through  = '1' and slot in ('62858868','66725501')
group by fenix_offerid,slot

    """.format(day=day)

    result = client.execute(sql_str)
    df = pd.DataFrame(list(result), columns=[ 'offer_id','slot', 'click'])
    return df
def truncate_table(cur,con):
    sql = "delete from fenix_click_cvr"
    cur.execute(sql)
    con.commit()


if __name__ == '__main__':
    from sys import argv

    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()

    date = datetime.datetime.now().strftime('%Y%m%d')
    print(date)
    df = execute_sql(date, client)

    cur = con.cursor()
    result_dict={}
    truncate_table(cur,con)
    for index,row in df.iterrows():
        key = str(row[0]) + "_" + str(row[1])
        value = row[2]
        udata_sql = "insert into fenix_click_cvr(`key`,`value`) values ('%s','%s') ON DUPLICATE KEY UPDATE `value`= '%s' " % (key,value,value)
        print(udata_sql)
        cur.execute(udata_sql)
        con.commit()
