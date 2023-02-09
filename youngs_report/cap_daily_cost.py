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
                SELECT channel AS channel,
                offer_id AS offer_id,
                country AS country,
                slot AS slot,
                substr(time_slot, 1, 8) AS day,
                COUNT(*) AS count
        FROM real_conversion
        WHERE toYYYYMMDD(utctime)= '{day}'
            AND event_type = '1'
            AND adtype IN ('38',
                            '39')
        GROUP BY channel,
                    offer_id,
                    country,
                    slot,
                    substr(time_slot, 1, 8)
        ORDER BY count DESC

    """.format(day=day)

    result = client.execute(sql_str)
    df = pd.DataFrame(list(result), columns=['channel', 'offer_id', 'country', 'slot', 'day', 'count'])
    return df

    # result_dict = {}
    # result = client.execute(sql_str)
    # for line in result:
    #     key = line[0] + "_" + line[1] + "_" + line[2] + "_" + line[3]
    #     value = line[5]
    #     result_dict[key] = value
    # return result_dict

def execute_php_sql():
   
    sql = """
            select channel,SUBSTRING_INDEX(adv_offer,'_',-1) as offer_id,country,offer_id as offer from new_offer
        """
    new_offer_df = pd.read_sql(sql, con)
    return new_offer_df



    

if __name__ == '__main__':
    from sys import argv
    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()
    date = datetime.datetime.now().strftime('%Y%m%d')
    df = execute_sql(date, client)
    new_offer_df = execute_php_sql()
    merge_data = pd.merge(df, new_offer_df, how="left", on=['channel', 'offer_id', 'country'])
    print(merge_data)
    columns = ['offer','slot','count']
    final_merge_data = merge_data[columns]

    result_dict={}
    for index,row in final_merge_data.iterrows():
        key = str(row[0]) + "_" + str(row[1])
        value = row[2]
        udata_sql = """
        insert into cap_daily_cost(`key`,`value`) values ('%s','%s') ON DUPLICATE KEY UPDATE `value`= '%s' 
        """ % (key,value,value)
        
