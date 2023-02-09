import pymysql
import pandas as pd
import psycopg2
import math
import datetime
php_config = {
    'host': 'php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com',
    'user': 'db_app',
    'password': 'Vee8lie3aiNee9sa',
    'database': 'new_ssp',
    'port': 3306
}
def get_php_data():


    condition_con = pymysql.connect(**php_config)

    con_sql = "select channel_offer,target_cvr from cloudmobi_sdk_cvr"

    php_df = pd.read_sql(con_sql,condition_con)
    return php_df
def get_realtime_data():
    date = str((datetime.date.today() - datetime.timedelta()).strftime('%Y%m%d%H'))
    red_config={
        'host':'cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com',
        'user':'cloudtech',
        'password':'Cloudtech2018',
        'database':'monitor',
        'port':5439
    }

    sql = """
    
        SELECT
	( CASE when imp.channel_offer is NULL THEN conv.channel_offer ELSE imp.channel_offer END ) AS channel_offer,
	click_w,
	COALESCE ( conv, 0 ) as conv,
	round(( case click_w when 0 then 0 else COALESCE ( conv, 0 ) /click_w end ),3) as cvr
FROM
	(
	SELECT
		concat ( concat ( channel, '_' ), offer ) AS channel_offer,
		round( CAST ( SUM ( COUNT ) AS FLOAT ) / 10000, 3 ) AS click_w 
	FROM
		impression 
	WHERE
		time_slot >= '%s' 
		AND adtype IN ( 3, 8, 9 ) 
	GROUP BY
		channel,
		offer 
	) imp
	FULL JOIN (
	SELECT
		concat ( concat ( channel, '_' ), offer ) AS channel_offer,
		SUM ( COUNT ) AS conv 
	FROM
	CONVERSION 
	WHERE
		time_slot >= '%s' 
	GROUP BY
		channel,
	offer 
	) conv ON imp.channel_offer = conv.channel_offer
    """ % (date,date)
    print(sql)
    red_con = psycopg2.connect(**red_config)
    red_df = pd.read_sql(sql,red_con)
    return red_df
def merge_data():
    date = str((datetime.date.today() - datetime.timedelta()).strftime('%Y%m%d%H'))
    php_df = get_php_data()
    realtime_df = get_realtime_data()
    merge_df = pd.merge(php_df,realtime_df,how='left',on=['channel_offer'])

    condition_con = pymysql.connect(**php_config)
    curs = condition_con.cursor()
    for index,row in merge_df.iterrows():

        channel_offer = row['channel_offer']
        click_w= row['click_w']
        conv = row['conv']
        cvr = row['cvr']
        target_cvr = row['target_cvr']
        if math.isnan(click_w):
            click_w=0
        if math.isnan(conv):
            conv=0
        if math.isnan(cvr):
            cvr=0
        if click_w==0 and conv>0:
            status = 1
        elif  math.isnan(cvr) or cvr<target_cvr:
            status = 2
        else:
            status = 1
        print(click_w,conv,cvr,status)
        update_sql = "update cloudmobi_sdk_cvr set click_w=%f,install=%d,cvr=%f,cvr_rate=%d where channel_offer = '%s'"%(click_w,conv,cvr,status,channel_offer)
        print(update_sql)
        try:
            curs.execute(update_sql)
            condition_con.commit()
        except:
            condition_con.rollback()

    condition_con.close()
merge_data()


