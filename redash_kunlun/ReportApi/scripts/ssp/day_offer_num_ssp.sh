export PGPASSWORD=Cloudtech2018
echo "$(date) begin" > log
day=$(date -d "3 days ago" "+%Y%m%d")
begin_day=$(date -d "1 days ago" "+%Y%m%d00")
end_day=$(date -d "1 days ago" "+%Y%m%d23")

echo $begin_day
echo $end_day

echo "select day,channel,click_offer_num,click_offer_rate,conv_offer_num,conv_offer_rate,conv_all_offer_rate,payout,payout_rate from (
SELECT DAY, channel, click_offer_num, SUM ( click_offer_num ) OVER () AS all_click_offer_num, round((round(cast(click_offer_num as numeric)/cast(all_click_offer_num as numeric),6))*100,4) as click_offer_rate, conv_offer_num, SUM ( conv_offer_num ) OVER () AS all_conv_offer_num, round((round(cast(conv_offer_num as numeric)/cast(all_conv_offer_num as numeric),6))*100,4) as conv_offer_rate, round((round(cast(conv_offer_num as numeric)/cast(all_click_offer_num as numeric),6))*100,4) as conv_all_offer_rate, payout, sum(payout) over() as all_payout, round((round(cast(payout as numeric)/cast(all_payout as numeric),6))*100,4) as payout_rate FROM ( SELECT A .DAY, A.channel, COUNT ( * ) AS click_offer_num, COUNT ( b.offer ) AS conv_offer_num, SUM ( payout ) AS payout  FROM ( SELECT SUBSTRING ( time_slot, 1, 8 ) AS DAY, channel, offer, SUM ( COUNT )  FROM impression  where time_slot>='$begin_day' and time_slot<='$end_day'  GROUP BY DAY, channel, offer  ) A LEFT JOIN ( SELECT SUBSTRING ( time_slot, 1, 8 ) AS DAY, channel, offer, SUM ( payout ) AS payout  FROM CONVERSION  WHERE time_slot>='$begin_day' and time_slot<='$end_day'  GROUP BY DAY, channel, offer  ) b ON ( A.DAY = b.DAY AND A.channel = b.channel AND A.offer = b.offer )  GROUP BY A.DAY, A.channel  ) A)b" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > day_offer_num_ssp.data

echo "load data local infile '/opt/ReportApi/scripts/ssp/day_offer_num_ssp.data' into table ssp_system.day_offer_num_ssp;" | mysql -hssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -umysql -pyeahmobi

