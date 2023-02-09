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
from ck_util import clickhouse_util

import redis

mysql_config = {
    'host': 'cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com',
    'user': 'redash',
    'password': 'Redash2018',
    'database': 'monitor',
    'port': 5439
}


def df_to_html(yibu_df, adjust_df):

    today = yibu_df.iloc[0]['time_slot']
    click = yibu_df.iloc[0]['click']
    yesterday_click = yibu_df.iloc[0]['yesterday_click']
    today_before_hour_click = yibu_df.iloc[0]['today_before_hour_click']
    # print(yibu_df)
    yibu_day_rate = round(yibu_df.iloc[0]['yibu_day_rate'], 2)
    yibu_hour_rate = round(yibu_df.iloc[0]['yibu_hour_rate'], 2)
    # -----------

    adjust_conv = adjust_df.iloc[0]['conv']

    d1_non = ''
    d1_non = d1_non + """
                <tr>
                    <td width="20" align="center">""" + str(today) + """</td>
                    <td width="80" align="center">""" + str(click) + """</td>
                    <td width="80" align="center">""" + str(yesterday_click) + """</td>
                    <td width="80" align="center">""" + str(today_before_hour_click) + """</td>
                    <td width="35" align="center">""" + str(yibu_day_rate) + "%" + """</td>
                    <td width="35" align="center">""" + str(yibu_hour_rate) + "%" + """</td>
                </tr>"""

    d4_non = ''

    d4_non = d4_non + """
    <tr>
                    <td align="center">""" + str(today) + """</td>
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
              <td width="80" align="center"><strong>当前小时点击数</strong></td>
              <td width="80" align="center"><strong>昨日同小时点击数</strong></td>
              <td width="80" align="center"><strong>前小时点击数</strong></td>
              <td width="35" align="center"><strong>环比</strong></td>
              <td width="35" align="center"><strong>同比</strong></td>
            </tr>""" + d1_non + """
            </table>
            <p><h1>Adjust转化详情</h1></p>
            <table width="45%" border="1" bordercolor="black" cellspacing="0" cellpadding="0">
            <tr>
              <td width="20" align="center"><strong>日期</strong></td>
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

def get_yibu_click(client):

    now_hour = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')
    last_hour = (datetime.datetime.today() - datetime.timedelta(hours=2)).strftime('%Y%m%d%H')
    yesterday_hour = (datetime.datetime.today() - datetime.timedelta(days=1, hours=1)).strftime('%Y%m%d%H')
    sql = """
         SELECT 
         time_slot1,
        click,
         yesterday_click,
            coalesce(today_before_hour_click,0) as today_before_hour_click,
            ( click - yesterday_click ) / yesterday_click * 100 AS yibu_day_rate,
            ( click - coalesce(today_before_hour_click,0) ) / coalesce(today_before_hour_click,1) * 100 AS yibu_hour_rate
        FROM
            (
            SELECT time_slot as time_slot1,
                'a' as j,
                SUM ( clk_count ) AS click 
            FROM
                real_buzz 
            WHERE
                time_slot = '{now_hour}' 
            GROUP BY
                time_slot 
            )
            A LEFT JOIN (
            SELECT time_slot,
               'a' as j,
                SUM ( clk_count) AS yesterday_click 
            FROM
                real_buzz 
            WHERE
                time_slot='{yesterday_hour}'
            GROUP BY
                time_slot 
            ) b ON A.j = b.j
            LEFT JOIN (
            SELECT time_slot,
                    'a' as j,
                    SUM (clk_count ) AS today_before_hour_click 
            FROM
                real_buzz 
            WHERE
                time_slot = '{last_hour}' 
            GROUP BY
                time_slot
            ) C ON A.j = C.j

    """.format(now_hour=now_hour, yesterday_hour=yesterday_hour, last_hour=last_hour)

    print(sql)
    result = client.execute(sql)
    df_result = pandas.DataFrame(list(result), columns=['time_slot','click', 'yesterday_click','today_before_hour_click','yibu_day_rate','yibu_hour_rate'])
    print(df_result)
    day_rate = df_result.iloc[0]['yibu_day_rate']
    hour_rate = df_result.iloc[0]['yibu_hour_rate']

    print(".............")
    print(day_rate, hour_rate)
    return df_result, day_rate, hour_rate




# 获取adjust数据
def get_adjust(client,time_slot):
    sql = """
            SELECT  count(*) as conv,sum(payout) as payout from real_conversion where attr_time>='{time_slot}'   and attribute_provider='Adjust'
    """.format(time_slot=time_slot)
    print(sql)

    result = client.execute(sql)
    df_result = pandas.DataFrame(list(result), columns=['conv', 'payout'])

    # print(df_result)
    conv = df_result.iloc[0]['conv']
    return df_result, conv


def connect_redis():
    import time
    key = 'adjust_warning'
    value = int(time.time())
    pool = redis.ConnectionPool(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, decode_responses=True)
    r = redis.Redis(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, decode_responses=True)
    r.set(key, value)  # 设置 name 对应的值
    print(key, r.get(key))  # 取出键 name 对应的值

import requests
def feishu(msg):
    url="http://18.163.115.182:7001/api/sendtochats?msg="+msg
    response=requests.get(url)
    print(response)


if __name__ == '__main__':
    realtime_con = psycopg2.connect(**mysql_config)
    today = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')
    adjust_today = (datetime.datetime.today() - datetime.timedelta(hours=6)).strftime('%Y%m%d%H')
    day = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d')
    hour = (datetime.datetime.today() - datetime.timedelta(hours=3)).strftime('%H')
    today_last_hour = (datetime.datetime.today() - datetime.timedelta(hours=2)).strftime('%Y%m%d%H')
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1, hours=1)).strftime('%Y%m%d%H')
    print(today)
    now_hour = (datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%H')
    print(now_hour)

    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()


    try:

        # 获取异步点击
        yibu_df, yibu_day_rate, yibu_hour_rate = get_yibu_click(client)
        # 获取adjust数据
        adjust_df, conv = get_adjust(client,adjust_today)

        if ((yibu_day_rate < (-30) and yibu_hour_rate < (-30))  or yibu_day_rate < (-50) or conv == 0):
            html = df_to_html(yibu_df, adjust_df)
            send_email(html)
            if conv == 0:
                feishu("Adjust BUZZ 当前转化异常 ！！！")
                print("Adjust BUZZ 当前转化异常 ！！")
                connect_redis()
            else:
                feishu("BUZZ 上小时点击异常！！！")
                pass
        else:
            print("一切____正常")
    except Exception as e:
        print(e.args)
        traceback.print_exc()
        if now_hour != "01" and now_hour != '00':
            print("代码运行错误")
            # send_email("代码运行错误,数据可能出现bug")

    # if rate >= 35:
    #    send_email(df_result)
