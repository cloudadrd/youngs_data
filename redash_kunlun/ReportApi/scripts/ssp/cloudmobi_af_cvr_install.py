# -*- coding: utf-8 -*-

import sys
# sys.path.append("/mnt/s3/IPO/bin/util/")
import pandas as pd
import pymysql
import numpy as np
import zipfile
import datetime
import hashlib
import os
import sys
from urllib import request
import gzip
import shutil
import psycopg2
from sqlalchemy import create_engine
report2_config = {
    'host': 'ssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com',
    'user': 'mysql',
    'password': 'yeahmobi',
    'database': 'report2',
    'port': 3306
}
realtime_config = {
    'host': 'cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com',
    'user': 'redash',
    'password': 'Redash2018',
    'database': 'monitor',
    'port': 5439
}

php_system_config = {
    'host': 'php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com',
    'user': 'db_app',
    'password': 'Vee8lie3aiNee9sa',
    'database': 'new_ssp',
    'port': 3306
}
from s3_util import S3File,db_util
def save_data(file_path):
    s3_key = 'cloudmobi_cvr/'
    bucket = "test-v4"
    file = file_path.split("/")[-1]
    key = s3_key + file
    f = S3File(bucket, key)
    f.copy_from(open(file_path, "rb"))
    print("over")
    return f

date = str((datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d'))
print(date)
con = pymysql.connect(**php_system_config)
sql="""
select *,DATE_FORMAT(install_time,'%Y%m%d%H') as time_slot from cloudmobi_af_cvr_install where DATE_FORMAT(install_time,'%Y%m%d') = '{a}' 
""".format(a=date)
print(sql)
php_df = pd.read_sql(sql,con)

# 删除当天数据
#red_con=psycopg2.connect(**realtime_config)
#cur = red_con.cursor()
#start_date=date+"00"
#end_date=date+"23"
#delete_sql = "delete from cloudmobi_af_cvr_install where time_slot >='%s'"%(start_date)
#print(delete_sql)
#cur.execute(delete_sql)
#red_con.commit()
#cur.close()
# 将数据插入表中
dir_name="/pdata/cvr_result/"
file_name="cvr_install_"+date+".csv"
php_df.to_csv(dir_name+file_name,index=None,header=None)
f=save_data(dir_name+file_name)
print('...')
s3_key = 'cloudmobi_cvr/'
bucket = "test-v4"
con = psycopg2.connect(**realtime_config)
con.set_client_encoding('utf8')
sql="copy cloudmobi_af_cvr_install from 's3://test-v4/{}{}' iam_role 'arn:aws:iam::955634015615:role/realtime-redshift' delimiter ',';".format(
                s3_key, file_name)
with con.cursor() as cur:
    cur.execute(sql)
    con.commit()

#yconnect = create_engine('postgresql+psycopg2://redash:Redash2018@cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com:5439/monitor?')
print("*****")
#pd.io.sql.to_sql(php_df, 'cloudmobi_af_cvr_install', yconnect, schema='public', if_exists='append',index=False)



