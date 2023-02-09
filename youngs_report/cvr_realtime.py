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


def execute_sql(self, day,hour,hour2, client):
    sql_str = """
        select click_tb.offer_id
        ,click_tb.country
        ,click_tb.channel
        ,click
        ,conversion
        ,round(conversion/click,2) cvr
        from
        (select offer_id,country,channel,round(sum(clk_count)/10000,2) click
        from real_buzz
        where time_slot >= '{day}00' and time_slot <= '{hour}' and clk_count > 0
        group by offer_id,country,channel) click_tb
        LEFT JOIN
        (select offer_id,country,channel,count(1) as conversion
        FROM real_conversion
        WHERE substr(toString(toYYYYMMDDhhmmss(utctime)),1,10) >= '{day}00' and substr(toString(toYYYYMMDDhhmmss(utctime)),1,10) <= '{hour2}' AND event_type = '1' AND adtype IN ('19','38')
        GROUP BY offer_id,country,channel) conversion_tb
        on click_tb.offer_id = conversion_tb.offer_id
        and click_tb.country = conversion_tb.country
        and click_tb.channel = conversion_tb.channel
    """.format(day=day,hour=hour,hour2=hour2)

    result = client.execute(sql_str)
    df = pd.DataFrame(list(result), columns=['offer_id', 'country', 'channel', 'click','conversion','cvr'])
    return df

    # result_dict = {}
    # result = client.execute(sql_str)
    # for line in result:
    #     key = line[0] + "_" + line[1] + "_" + line[2] + "_" + line[3]
    #     value = line[5]
    #     result_dict[key] = value
    # return result_dict

def execute_php_sql(self):
    
    sql = """
            select channel,SUBSTRING_INDEX(adv_offer,'_',-1) as offer_id,country,offer_id as offer from new_offer
        """
    new_offer_df = pd.read_sql(sql, con)
    return new_offer_df





if __name__ == '__main__':
    from sys import argv
    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()
    start_date = datetime.datetime.now().strftime('%Y%m%d')
    hour = (datetime.datetime.now()- datetime.timedelta(hours=1)).strftime('%Y%m%d%H')
    hour2 = (datetime.datetime.now()- datetime.timedelta(hours=2)).strftime('%Y%m%d%H')
    date = datetime.datetime.now().strftime('%Y%m%d')
    df = execute_sql(start_date,hour,hour2, client)
    new_offer_df = execute_php_sql()
    merge_data = pd.merge(df, new_offer_df, how="left", on=['channel', 'offer_id', 'country'])
    print(merge_data)
    columns = ['offer','click','conversion','cvr']
    final_merge_data = merge_data[columns]
    final_merge_data = final_merge_data.loc[final_merge_data['offer'].notnull()]
    cur = con.cursor()
    for index,row in final_merge_data.iterrows():
        key = str(row[0])
        value = str(row[1]) + "_" + str(row[2]) + "_" + str(row[3])
        udata_sql = "insert into realtime_cvr(`key`,`value`) values ('%s','%s') ON DUPLICATE KEY UPDATE `value`= '%s' " % (key,value,value)
        print(udata_sql)
        cur.execute(udata_sql)
        con.commit()