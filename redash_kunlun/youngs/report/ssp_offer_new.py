import os
import pandas as pd
import psycopg2
import datetime
from sqlalchemy import create_engine
import pymysql
redshift_config = {
    'host': 'cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com',
    'user': 'cloudtech',
    'password': 'Cloudtech2018',
    'database': 'monitor',
    'port': 5439
}
con = psycopg2.connect(**redshift_config)
con.set_client_encoding('utf8')

yesterday=datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(1),'%Y%m%d')
b_yesterday=yesterday+'00'
e_yesterday=yesterday+'23'
sql='''
 select ta.*,tb.click from (SELECT
	concat(concat ( channel,'_' ),offer )as offer,
	platform,
		country,
	SUM ( COUNT ) as count,
	SUM ( payout ) AS revenue,
	to_date(substring(time_slot,1,8),'YYYYMMDD') as date,
	pkg FROM
CONVERSION
WHERE
	time_slot >= {a} and time_slot<={b}  AND pkg != '' GROUP BY date,concat(concat ( channel,'_' ),offer ), platform, pkg, country )ta
	left join
	(select
	concat(concat ( channel,'_' ),offer) as offer,
	country,
	platform,
	sum(count) as click,
	to_date(substring(time_slot,1,8),'YYYYMMDD') as date,
	pkg
	from
	impression
	where
	time_slot>={a}
	and time_slot <= {b} and pkg !='' group by date,concat(concat ( channel,'_' ),offer ), platform, pkg, country)tb
	on ta.offer=tb.offer and ta.country=tb.country and ta.platform=tb.platform and ta.pkg=tb.pkg and ta.date=tb.date
'''.format(a=b_yesterday,b=e_yesterday)
df = pd.read_sql_query(sql,con)


yconnect = create_engine('mysql+pymysql://mysql:yeahmobi@ssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com:3306/reports?charset=utf8')
   # pandas,表名，链接，库名，数据添加模式（追加），不存储索引
pd.io.sql.to_sql(df, 'ssp_offer_new', yconnect, schema='reports', if_exists='append', index=False)