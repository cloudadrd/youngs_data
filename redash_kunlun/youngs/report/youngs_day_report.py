from clickhouse_driver import Client
from ck_util import clickhouse_util
import pandas as pd
import numpy as np
import sys
from datetime import datetime, timedelta
import boto3
from sqlalchemy import create_engine
import time
import pymysql
clickhouse_client = clickhouse_util()
client=clickhouse_client.return_client()
mysql_config = {
    'host': 'php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com',
    'user': 'db_app',
    'password': 'Vee8lie3aiNee9sa',
    'db': 'new_ssp',
    'port': 3306
}

def delete_mysql(day) :
    db = pymysql.connect(**mysql_config)
    cursor = db.cursor()
    conn = cursor.execute
    delete_now= "delete from new_ssp.youngs_all_report where day='{time_slot}'".format(time_slot=day)
    #delete_before= "delete from new_ssp.youngs_all_report where day like '{before_day}%'".format(before_day=before_day)
    #print(delete_now + "---------" + delete_before)
    cursor.execute(delete_now)
    db.commit()
    #cursor.execute(delete_before)
    #db.commit()
    db.close()
    cursor.close()

def build_insert_data_sql(day):


        sql = """
            SELECT '{time_slot}' day
        , multiIf(length(click_tb.country) > 1 ,click_tb.country ,length(conversion_tb.country) > 1 ,conversion_tb.country, length(reject_tb.country) > 1 ,conversion_tb.country ,'') AS country
    , multiIf(length(click_tb.offer_id) > 1 ,click_tb.offer_id ,length(conversion_tb.offer_id) > 1 ,conversion_tb.offer_id, length(reject_tb.offer_id) > 1 ,conversion_tb.offer_id ,'') AS offer
    , multiIf(length(click_tb.channel) > 1 ,click_tb.channel ,length(conversion_tb.channel) > 1 ,conversion_tb.channel, length(reject_tb.channel) > 1 ,conversion_tb.channel ,'') AS channel
    , multiIf(length(click_tb.offer_pkg) > 1 ,click_tb.offer_pkg ,length(conversion_tb.offer_pkg) > 1 ,conversion_tb.offer_pkg, length(reject_tb.offer_pkg) > 1 ,conversion_tb.offer_pkg ,'') AS pkg
    , multiIf(length(click_tb.platform) > 1 ,click_tb.platform ,length(conversion_tb.platform) > 1 ,conversion_tb.platform, length(reject_tb.platform) > 1 ,conversion_tb.platform ,'') AS platform
	, sum(click) AS click_all, sum(imp) AS impression
	, sum(install_buzz) AS install_buzz, sum(conv) AS conv
	, sum(reject) AS reject
	, round(sum(revenue), 2) AS revenue
	, round(sum(cost), 2) AS cost
FROM (
	SELECT offer_id, country, channel, max(offer_pkg) AS offer_pkg
		, max(platform) AS platform, sum(click) AS click
		, sum(imp) AS imp
	FROM (
		SELECT offer_id, country, channel, offer_pkg, platform
			, CAST(sum(clk_count) AS INTEGER) AS click, sum(imp_count) AS imp
		FROM real_buzz
		WHERE time_slot like '{time_slot}%%'
			AND offer_id != ''
			AND country != ''
			AND channel != ''
			AND offer_pkg NOT LIKE '%%lazada%%'
		GROUP BY offer_id, country, channel, offer_pkg, platform
		UNION ALL
		SELECT CASE
				WHEN adtype = '39' THEN splitByString('_', offer_id)[2]
				ELSE offer_id
			END AS offer_id, country, channel, '' AS offer_pkg, '' AS platform
			, CAST(count(1) AS INTEGER) AS click, 0 AS imp
		FROM real_fenix_click
		WHERE time_slot like '{time_slot}%%'
			AND click_through = '1'
			AND country != ''
			AND channel != ''
			AND offer_pkg NOT LIKE '%%lazada%%'
		GROUP BY CASE
				WHEN adtype = '39' THEN splitByString('_', offer_id)[2]
				ELSE offer_id
			END, country, channel, adtype, offer_id, offer_pkg, platform
		UNION ALL
		SELECT offer_id, country, channel, '' AS offer_pkg, '' AS platform
			, CAST(sum(clk_count) AS INTEGER) AS click, 0 AS imp
		FROM real_buzz_sync
		WHERE time_slot like '{time_slot}%%'
			AND country != ''
			AND channel != ''
			AND offer_pkg NOT LIKE '%%lazada%%'
		GROUP BY offer_id, country, channel, offer_pkg, platform
	)
	GROUP BY offer_id, country, channel
) click_tb
	FULL JOIN (
		SELECT country, platform, offer_pkg, offer_id, channel
			, sum(CASE
				WHEN event_type = '1' THEN 1
				ELSE 0
			END) AS install_buzz
			, sum(CASE
				WHEN event_type = '2' THEN 1
				ELSE 0
			END) AS conv
			, sum(payout) AS revenue, sum(cost) AS cost
		FROM real_conversion
		WHERE time_slot like '{time_slot}%%'
			AND offer_pkg != ''
			AND offer_id != ''
		GROUP BY country, platform, offer_pkg, offer_id, channel
	) conversion_tb
	ON click_tb.offer_id = conversion_tb.offer_id
		AND click_tb.country = conversion_tb.country
	FULL JOIN (
		SELECT country, platform, offer_pkg, offer_id, channel
			, count(1) AS reject
		FROM reject_conversion
		WHERE time_slot like '{time_slot}%%'
		GROUP BY country, platform, offer_pkg, offer_id, channel
	) reject_tb
	ON conversion_tb.offer_id = reject_tb.offer_id
		AND conversion_tb.country = reject_tb.country
GROUP BY multiIf(length(click_tb.country) > 1 ,click_tb.country ,length(conversion_tb.country) > 1 ,conversion_tb.country, length(reject_tb.country) > 1 ,conversion_tb.country ,'')
    , multiIf(length(click_tb.offer_id) > 1 ,click_tb.offer_id ,length(conversion_tb.offer_id) > 1 ,conversion_tb.offer_id, length(reject_tb.offer_id) > 1 ,conversion_tb.offer_id ,'')
    , multiIf(length(click_tb.channel) > 1 ,click_tb.channel ,length(conversion_tb.channel) > 1 ,conversion_tb.channel, length(reject_tb.channel) > 1 ,conversion_tb.channel ,'')
    , multiIf(length(click_tb.offer_pkg) > 1 ,click_tb.offer_pkg ,length(conversion_tb.offer_pkg) > 1 ,conversion_tb.offer_pkg, length(reject_tb.offer_pkg) > 1 ,conversion_tb.offer_pkg ,'')
    , multiIf(length(click_tb.platform) > 1 ,click_tb.platform ,length(conversion_tb.platform) > 1 ,conversion_tb.platform, length(reject_tb.platform) > 1 ,conversion_tb.platform ,'')
            """.format(time_slot=day)
        print(sql)
        result = client.execute(sql)
        columns=['day','country','offer','channel','pkg','platform','click_all','impression','install_buzz','conv','reject','revenue','cost']
        df =pd.DataFrame(list(result),columns=columns)
        df.to_csv("/opt/youngs/test.csv")
        conn = create_engine(
            'mysql+pymysql://db_app:Vee8lie3aiNee9sa@php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com:3306/new_ssp?charset=utf8')
        #delete_sql = "delete from new_ssp.youngs_hour_report where time_slot={time_slot}".format(time_slot=day)
        #print(delete_sql)
        #pd.read_sql_query(delete_sql,con=conn)
        pd.io.sql.to_sql(df, 'youngs_all_report', conn, schema='new_ssp', if_exists='append', index=False)

        return df



if __name__ == '__main__':
    if len(sys.argv) > 1:
        inter = int(sys.argv[1])
        day = str((datetime.today() - timedelta(days=inter)).strftime('%Y%m%d'))


    else:
        day = str((datetime.today() - timedelta(days=1) ).strftime('%Y%m%d'))

    delete_day = str((datetime.today() - timedelta(days=14) ).strftime('%Y%m%d'))
    delete_mysql(day )
    clickhouse_df = build_insert_data_sql(day)