# -*- coding: utf-8 -*-

import pymysql
import datetime
import pandas
import math
import smtplib
import collections
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
import traceback
import psycopg2

import redis
mysql_config = {
    'host': 'cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com',
    'user': 'redash',
    'password': 'Redash2018',
    'database': 'monitor',
    'port': 5439
}


def df_to_html(yibu_df, fuyu_df, conv_df, adjust_df):
    today = yibu_df.iloc[0]['day']
    utc_hour = yibu_df.iloc[0]['hour']
    click = yibu_df.iloc[0]['click']
    yesterday_click = yibu_df.iloc[0]['yesterday_click']
    today_before_hour_click = yibu_df.iloc[0]['today_before_hour_click']
    # print(yibu_df)
    yibu_day_rate = round(yibu_df.iloc[0]['yibu_day_rate'], 2)
    yibu_hour_rate = round(yibu_df.iloc[0]['yibu_hour_rate'], 2)
    # -----------
    fuyu_click = fuyu_df.iloc[0]['click']
    fuyu_yesterday_click = fuyu_df.iloc[0]['yesterday_click']
    fuyu_today_before_hour_click = fuyu_df.iloc[0]['today_before_hour_click']
    fuyu_day_rate = round(fuyu_df.iloc[0]['day_rate'], 2)
    fuyu_hour_rate = round(fuyu_df.iloc[0]['hour_rate'], 2)

    # -----------
    payout = conv_df.iloc[0]['payout']
    adjust_click = adjust_df.iloc[0]['click']
    adjust_conv = adjust_df.iloc[0]['conv']

    d1_non = ''
    d1_non = d1_non + """
                <tr>
                    <td width="20" align="center">""" + str(today) + """</td>
                    <td width="20" align="center">""" + str(utc_hour) + """</td>
                    <td width="80" align="center">""" + str(click) + """</td>
                    <td width="80" align="center">""" + str(yesterday_click) + """</td>
                    <td width="80" align="center">""" + str(today_before_hour_click) + """</td>
                    <td width="35" align="center">""" + str(yibu_day_rate) + "%" + """</td>
                    <td width="35" align="center">""" + str(yibu_hour_rate) + "%" + """</td>
                </tr>"""

    d2_non = ''
    d2_non = d2_non + """
                <tr>
                    <td width="20" align="center">""" + str(today) + """</td>
                    <td width="20" align="center">""" + str(utc_hour) + """</td>
                    <td width="80" align="center">""" + str(fuyu_click) + """</td>
                    <td width="80" align="center">""" + str(fuyu_yesterday_click) + """</td>
                    <td width="80" align="center">""" + str(fuyu_today_before_hour_click) + """</td>
                    <td width="35" align="center">""" + str(fuyu_day_rate) + "%" + """</td>
                    <td width="35" align="center">""" + str(fuyu_hour_rate) + "%" + """</td>
                </tr>"""

    d3_non = ''

    d3_non = d3_non + """
    <tr>
                    <td align="center">""" + str(today) + """</td>
                    <td align="center">""" + str(utc_hour) + """</td>
                    <td width="60" align="center">""" + str(payout) + """</td>
    </tr>"""

    d4_non = ''

    d4_non = d4_non + """
    <tr>
                    <td align="center">""" + str(today) + """</td>
                    <td align="center">""" + str(utc_hour) + """</td>
                    <td width="60" align="center">""" + str(adjust_click) + """</td>
                    <td width="60" align="center">""" + str(adjust_conv) + """</td>
    </tr>"""

    # web页面内容
    html = """\
            <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <body>
            <div id="container">
            <p><h1>直链点击详情</h1></p>
            <div id="content">
             <table width="85%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
              <td width="20" align="center"><strong>日期</strong></td>
              <td width="20" align="center"><strong>当前小时</strong></td>
              <td width="80" align="center"><strong>当前小时点击数</strong></td>
              <td width="80" align="center"><strong>昨日同小时点击数</strong></td>
              <td width="80" align="center"><strong>前小时点击数</strong></td>
              <td width="35" align="center"><strong>环比</strong></td>
              <td width="35" align="center"><strong>同比</strong></td>
            </tr>""" + d1_non + """
            </table>
            </table>
            <p><h1>SDK点击详情</h1></p>
            <table width="85%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
                <td width="20" align="center"><strong>日期</strong></td>
              <td width="20" align="center"><strong>当前小时</strong></td>
              <td width="80" align="center"><strong>当前小时点击数</strong></td>
              <td width="80" align="center"><strong>昨日同小时点击数</strong></td>
              <td width="80" align="center"><strong>前小时点击数</strong></td>
              <td width="35" align="center"><strong>环比</strong></td>
              <td width="35" align="center"><strong>同比</strong></td>
            </tr>""" + d2_non + """
            </table>

            <p><h1>收入详情</h1></p>
            <table width="45%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
              <td width="20" align="center"><strong>日期</strong></td>
              <td width="80" align="center"><strong>小时</strong></td>
              <td width="80" align="center"><strong>收入</strong></td>
            </tr>""" + d3_non + """
            </table>
            <p><h1>Adjust转化详情</h1></p>
            <table width="45%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
              <td width="20" align="center"><strong>日期</strong></td>
              <td width="80" align="center"><strong>小时</strong></td>
                 <td width="80" align="center"><strong>点击</strong></td>
              <td width="80" align="center"><strong>转化数</strong></td>
            </tr>""" + d4_non + """

            </table>

            </div>
            </div>
            </div>
        </body>
        </html>
            """
    return html


def send_email(html):
    # mail_host = "mail.ndpmedia.com"  # 设置服务器
    mail_host = "smtp.163.com"
    mail_user = "17530152552@163.com"  # 用户名
    mail_pass = "EKBFBMFKQJCJMPXH"  # 口令

    sender = '17530152552@163.com'

    # 添加接收人
    # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    receiver = '17530152552@163.com'
    receivers = ['gedeon.guo@yeahmobi.com', 'simone.wu@yeahmobi.com', 'summer.he@yeahmobi.com',
                 'aaron.song@yeahmobi.com', 'jason.chenl@yeahmobi.com', 'yvonne.pan@yeahmobi.com',
                 'genesis.ge@yeahmobi.com', 'meredith.chen@yeahmobi.com']
    # receivers=['gedeon.guo@yeahmobi.com','jason.chenl@yeahmobi.com']
    # 定义邮件内容
    msgRoot = MIMEMultipart('related')
    msgRoot['From'] = mail_user
    msgRoot['To'] = receiver
    from datetime import datetime, date, timedelta
    one_day = (date.today() + timedelta(days=-1)).strftime("%Y%m%d")
    subject = '数据发生异常({b})'.format(b=one_day)
    msgRoot['Subject'] = Header(subject, 'utf-8')
    msgAlternative = MIMEMultipart('alternative')
    msgRoot.attach(msgAlternative)
    msgAlternative.attach(MIMEText(html, 'html', 'utf-8'))
    # 定义图片 ID，在 HTML 文本中引用
    smtpObj = smtplib.SMTP()
    smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
    smtpObj.login(mail_user, mail_pass)
    smtpObj.sendmail(sender, receivers, msgRoot.as_string())
    print("邮件发送成功")


# 异步点击

def get_yibu_click():
    today = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d')
    now_hour = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%H')
    last_hour = (datetime.datetime.today() - datetime.timedelta(hours=2)).strftime('%H')
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1, hours=1)).strftime('%Y%m%d')
    if last_hour == "00":
        last_hour = '0'
    # print
    print(last_hour,today)
    if last_hour=='0':
        today = (datetime.datetime.today() - datetime.timedelta(hours=3)).strftime('%Y%m%d')
    sql = """

    SELECT DAY
	,
	HOUR,
	click,
	yesterday_click,
	coalesce(today_before_hour_click,0) as today_before_hour_click,
	( click - yesterday_click ) / CAST ( yesterday_click AS NUMERIC ) * 100 AS yibu_day_rate,
	( click - coalesce(today_before_hour_click,0) ) / CAST ( coalesce(today_before_hour_click,1) AS NUMERIC ) * 100 AS yibu_hour_rate 
FROM
	(
	SELECT DATE AS DAY
		,
		EXTRACT ( HOUR FROM created ) AS HOUR,
	    'a' as j,
		SUM ( COUNT ) AS click 
	FROM
		yibu_click_new 
	WHERE
		DATE = '{today}' 
		AND EXTRACT ( HOUR FROM created ) = '{now_hour}' 
	GROUP BY
		DATE,
		EXTRACT ( HOUR FROM created ) 
	)
	A LEFT JOIN (
	SELECT DATE
		,
		EXTRACT ( HOUR FROM created ) AS yesterday_hour,
		SUM ( COUNT ) AS yesterday_click 
	FROM
		yibu_click_new 
	WHERE
		DATE = '{yesterday}' 
		AND EXTRACT ( HOUR FROM created ) = '{now_hour}' 
	GROUP BY
		DATE,
		EXTRACT ( HOUR FROM created ) 
	) b ON A.HOUR = b.yesterday_hour
	LEFT JOIN (
	SELECT DATE
		,
			'a' as j,
		EXTRACT ( HOUR FROM created ) AS last_hour,
	        SUM ( COUNT ) AS today_before_hour_click 
	FROM
		yibu_click_new 
	WHERE
		DATE = '{today}' 
		AND EXTRACT ( HOUR FROM created ) = '{last_hour}' 
	GROUP BY
		DATE,
	EXTRACT ( HOUR FROM created ) 
	) C ON A.j = C.j

    """.format(today=today, yesterday=yesterday, last_hour=last_hour, now_hour=now_hour)

    print(sql)
    df_result = pandas.read_sql(sql, realtime_con)
    day_rate = df_result.iloc[0]['yibu_day_rate']
    hour_rate = df_result.iloc[0]['yibu_hour_rate']

    print(".............")
    print(day_rate, hour_rate)
    return df_result, day_rate, hour_rate


# 服预点击
def get_fuyu_click(now, yesterday_hour, today_last_hour):
    # print(now, yesterday_hour, today_last_hour)
    sql = """
        SELECT 
    	 DAY,
    	HOUR,
    	click,
    	yesterday,
    	yesterday_click,
    	today_before_hour_click,
    	( click - yesterday_click ) / CAST ( yesterday_click AS NUMERIC ) * 100 AS day_rate,
    	(click-today_before_hour_click)/CAST( today_before_hour_click as NUMERIC )*100 as hour_rate 

    	FROM
    	(
    	SELECT SUBSTRING
    		( time_slot, 1, 8 ) AS DAY,
    		SUBSTRING ( time_slot, 9, 10 ) AS HOUR,
    		SUM ( COUNT ) AS click 
    	FROM
    		impression 
    	WHERE
    		time_slot = '%s'
    	GROUP BY
    		time_slot 
    	)
    	A LEFT JOIN (
    	SELECT SUBSTRING
    		( time_slot, 1, 8 ) AS yesterday,
    		SUBSTRING ( time_slot, 9, 10 ) AS yesterday_hour,
    		SUM ( COUNT ) AS yesterday_click 
    	FROM
    		impression 
    	WHERE
    		time_slot = '%s'
    	GROUP BY
    	time_slot 
    	) b ON A.HOUR = b.yesterday_hour
    	left join (
    	SELECT SUBSTRING
    		( time_slot, 1, 8 ) AS today,
    		SUBSTRING ( time_slot, 9, 10 ) AS today_before_hour,
    		SUM ( COUNT ) AS today_before_hour_click 
    	FROM
    		impression 
    	WHERE
    		time_slot = '%s'
    	GROUP BY
    	time_slot)c on A.day=c.today
        """ % (now, yesterday_hour, today_last_hour)
    # print(sql)
    df_result = pandas.read_sql(sql, realtime_con)
    day_rate = df_result.iloc[0]['day_rate']
    hour_rate = df_result.iloc[0]['hour_rate']

    print(".............")
    # print(day_rate, hour_rate)
    return df_result, day_rate, hour_rate


# 收入预警

def get_conversion(today):
    sql = """
    	SELECT 
		SUBSTRING( time_slot, 1, 8 ) AS DAY,
		SUBSTRING ( time_slot, 9, 10 ) AS HOUR,
		SUM ( payout ) AS payout 
	FROM
		conversion 
	WHERE
		time_slot = '%s' 
	GROUP BY
		time_slot
    """ % today

    # print(sql)
    df_result = pandas.read_sql(sql, realtime_con)
    payout = df_result.iloc[0]['payout']
    # print(payout)
    return df_result, payout


# 获取adjust数据
def get_adjust(day, hour, time_slot):
    sql = """
            select click_day,sum(click) as click,sum(conv) as conv from (
            select ta.click_day,ta.click_hour,COALESCE(sum(ta.click),0) as click,COALESCE(sum(tb.conv ),0) as conv from
        (
        select date as click_day,substring(created,12,2) AS click_hour,offer,sum(count)click  from yibu_click_new  
        where date>='{day}' and  date<='{day}' and pid= ''  and substring(created,12,2)>='{hour}'
        group by click_day,click_hour,offer having click>10000)ta
        left join
        (
        select
        substring(attr_hour, 1, 8) AS click_day,offer,substring(attr_hour,9,2) AS click_hour,sum(count) conv from conversion
        where attr_hour>='{time_slot}' and adtype='19'
        group by click_day,click_hour,offer
        )tb
        on (ta.click_day=tb.click_day and  ta.click_hour=tb.click_hour and ta.offer=tb.offer ) group by 	ta.click_day,
	ta.click_hour
        order by click_hour)a group by click_day
    """.format(day=day, hour=hour, time_slot=time_slot)
    print(sql)
    df_result = pandas.read_sql(sql, realtime_con)
    # print(df_result)
    conv = df_result.iloc[0]['conv']
    return df_result, conv

def connect_redis():
    import time
    key='adjust_warning'
    value = int(time.time())
    pool = redis.ConnectionPool(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, decode_responses=True)
    r = redis.Redis(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, decode_responses=True)
    r.set(key, value)  # 设置 name 对应的值
    print(key,r.get(key))  # 取出键 name 对应的值

if __name__ == '__main__':
    realtime_con = psycopg2.connect(**mysql_config)
    today = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')
    adjust_today=(datetime.datetime.today() - datetime.timedelta(hours=3)).strftime('%Y%m%d%H')
    day = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d')
    hour = (datetime.datetime.today() - datetime.timedelta(hours=3)).strftime('%H')
    today_last_hour = (datetime.datetime.today() - datetime.timedelta(hours=2)).strftime('%Y%m%d%H')
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1, hours=1)).strftime('%Y%m%d%H')
    print(today)
    now_hour = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%H')
    print(now_hour)
    try:
        # 获取服预点击
        fuyu_df, fuyu_day_rate, fuyu_hour_rate = get_fuyu_click(today, yesterday, today_last_hour)
        # 获取异步点击
        yibu_df, yibu_day_rate, yibu_hour_rate = get_yibu_click()
        # 获取收入
        conv_df, payout = get_conversion(today)
        # 获取adjust数据
        adjust_df, conv = get_adjust(day, hour, adjust_today)

        
        if ((yibu_day_rate < (-30) and yibu_hour_rate < (-30)) or (
                fuyu_day_rate < (-30) and fuyu_hour_rate < (-30)) or (
                payout <= 100) or yibu_day_rate < (-50) or fuyu_day_rate < (-50) or conv == 0):
            html = df_to_html(yibu_df, fuyu_df, conv_df, adjust_df)
            send_email(html)
            if conv==0:
                connect_redis()
            else:
                pass
        else:
            print("一切____正常")
    except Exception as e:
        print(e.args)
        traceback.print_exc()
        if now_hour!="01" and now_hour!='00':
            print("代码运行错误")
            #send_email("代码运行错误,数据可能出现bug")

    # if rate >= 35:
    #    send_email(df_result)
