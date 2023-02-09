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
import redis

realtime_config = {
    'host': 'cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com',
    'user': 'redash',
    'password': 'Redash2018',
    'database': 'monitor',
    'port': 5439
}

red_con=psycopg2.connect(**realtime_config)
cur = red_con.cursor()
date = str(datetime.datetime.today().strftime('%Y%m%d00'))
date_log = str(datetime.datetime.today().strftime('%Y%m%d%H %M'))
print(date_log + '  data is :')
delete_sql = '''select  slot,sum(count) c from impression where time_slot>= '%s' and slot in ('70403608','42015747','54839808'
,'32314900','41571384','54478080','89851019','31070908','29342889','43396577','67349955'
,'63227553','94855622','85037174','24721785','78681516') and visible='1' and adtype!='12' group by  slot'''%(date)
cur.execute(delete_sql)
res=cur.fetchall()
redis_cnn = redis.Redis(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, db=0)
list = ['70403608','42015747','54839808','32314900','41571384','54478080','89851019','31070908','29342889','43396577','67349955','63227553','94855622','85037174','24721785']
print(res)
for i in list :
    redis_cnn.set( 'summer_' + str(i) , str(0))

for row in res :
    slot = 'summer_' + str(row[0])
    value = int(row[1])/0.9
    if str(row[0]) == '54478080' :
        print(str(row[0]) + '\t' + str(int(value)) )
    redis_cnn.set( slot , str(int(value)))
    
