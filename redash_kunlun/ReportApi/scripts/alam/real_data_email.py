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
import psycopg2

import redis

mysql_config = {
    'host': 'cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com',
    'user': 'redash',
    'password': 'Redash2018',
    'database': 'monitor',
    'port': 5439
}


def df_to_html(click_df, imp_df):
    # -------------真实点击
    today = click_df.iloc[0]['day']
    utc_hour = click_df.iloc[0]['hour']
    click_count = click_df.iloc[0]['count']
    imp_count = imp_df.iloc[0]['count']
    # ----------- 真实展示

    d3_non = ''

    d3_non = d3_non + """
    <tr>
                    <td align="center">""" + str(today) + """</td>
                    <td align="center">""" + str(utc_hour) + """</td>
                    <td width="60" align="center">""" + str(click_count) + """</td>
    </tr>"""

    d4_non = ''

    d4_non = d4_non + """
    <tr>
                    <td align="center">""" + str(today) + """</td>
                    <td align="center">""" + str(utc_hour) + """</td>
                    <td width="60" align="center">""" + str(imp_count) + """</td>
    </tr>"""

    # web页面内容
    html = """\
            <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <body>
            <div id="container">
            <div id="content">

            <p><h1>真实点击详情</h1></p>
            <table width="45%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
              <td width="20" align="center"><strong>日期</strong></td>
              <td width="80" align="center"><strong>小时</strong></td>
              <td width="80" align="center"><strong>真实点击</strong></td>
            </tr>""" + d3_non + """
            </table>
            <p><h1>真实展示详情</h1></p>
            <table width="45%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
              <td width="20" align="center"><strong>日期</strong></td>
              <td width="80" align="center"><strong>小时</strong></td>
              <td width="80" align="center"><strong>真实展示</strong></td>
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
    #receivers = ['gedeon.guo@yeahmobi.com', 'simone.wu@yeahmobi.com', 'summer.he@yeahmobi.com',
              #   'aaron.song@yeahmobi.com', 'jason.chenl@yeahmobi.com', 'yvonne.pan@yeahmobi.com',
               #  'genesis.ge@yeahmobi.com', 'meredith.chen@yeahmobi.com']
    receivers=['gedeon.guo@yeahmobi.com','jason.chenl@yeahmobi.com']
    # 定义邮件内容
    msgRoot = MIMEMultipart('related')
    msgRoot['From'] = mail_user
    msgRoot['To'] = receiver
    from datetime import datetime, date, timedelta
    one_day = (date.today() + timedelta(days=-1)).strftime("%Y%m%d")
    subject = '真实数据发生异常({b})'.format(b=one_day)
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


# 收入预警

def get_real_click(today):
    sql = """
    	SELECT 
		SUBSTRING( time_slot, 1, 8 ) AS DAY,
		SUBSTRING ( time_slot, 9, 10 ) AS HOUR,
		SUM ( count ) AS count 
	FROM
		click 
	WHERE
		time_slot = '%s' 
	GROUP BY
		time_slot
    """ % today

    # print(sql)
    df_result = pandas.read_sql(sql, realtime_con)
    count = df_result.iloc[0]['count']
    # print(payout)
    return df_result, count


# 获取adjust数据
def get_real_imp(today):
    sql = """
        SELECT 
		SUBSTRING( time_slot, 1, 8 ) AS DAY,
		SUBSTRING ( time_slot, 9, 10 ) AS HOUR,
		SUM ( count ) AS count 
	FROM
		impression 
	WHERE
		time_slot = '%s'  and adtype='16'
	GROUP BY
		time_slot
    """%today
    print(sql)
    df_result = pandas.read_sql(sql, realtime_con)
    # print(df_result)
    count = df_result.iloc[0]['count']
    return df_result, count


def connect_redis():
    import time
    key = 'adjust_warning'
    value = int(time.time())
    pool = redis.ConnectionPool(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, decode_responses=True)
    r = redis.Redis(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, decode_responses=True)
    r.set(key, value)  # 设置 name 对应的值
    print(key, r.get(key))  # 取出键 name 对应的值


if __name__ == '__main__':
    realtime_con = psycopg2.connect(**mysql_config)
    today = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')
    day = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d')
    hour = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%H')
    today_last_hour = (datetime.datetime.today() - datetime.timedelta(hours=2)).strftime('%Y%m%d%H')
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1, hours=1)).strftime('%Y%m%d%H')
    print(today)
    now_hour = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%H')
    print(now_hour)
    try:
        # 获取真实点击
        click_df, click_count = get_real_click(today)
        # 获取adjust数据
        imp_df, imp_count = get_real_imp(today)

        if (click_count<100 or imp_count<1000):
            html = df_to_html(click_df, imp_df)
            send_email(html)
        else:
            print("一切____正常")
    except Exception as e:
        print(e.args)
        if now_hour != "01":
            send_email("真实数据代码运行错误,数据可能出现bug")

    # if rate >= 35:
    #    send_email(df_result)
