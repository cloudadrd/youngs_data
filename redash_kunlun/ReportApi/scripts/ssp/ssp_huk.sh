export PGPASSWORD=Cloudtech2018
if [ -n "$1" ]; then
    start_date=$1"00"
    end_date=$1"23"
    today=$1
else
    start_date=$(date -d "1 days ago" "+%Y%m%d00")
    end_date=$(date -d "1 days ago" "+%Y%m%d23")
    today=$(date -d "1 days ago" "+%Y%m%d")
fi
echo "	select '$today' as date ,ta.platform, ta.adtype, ta.country, ta.channel, ta.offer, ta.imp, tb.click from(select platform,adtype,country,channel,offer,sum(count)imp from impression where time_slot >= '$start_date' and time_slot <= '$end_date' and channel in('huk' ,'bin','rcm4' ,'mbg','real') and adtype in ('0','1', '2' ,'5' , '6', '7', '10', '13', '14', '15' ,'16' , '17') GROUP BY platform,adtype,country,channel,offer)ta left join(select platform,adtype,country,channel,offer,sum(count) click from click where time_slot >= '$start_date' and time_slot <= '$end_date' and channel in('huk' ,'bin','rcm4' ,'mbg','real') and adtype in ('0','1', '2' ,'5' , '6', '7', '10', '13', '14', '15' ,'16' , '17') GROUP BY platform,adtype,country,channel,offer) tb ON (ta.platform = tb.platform and ta.adtype = tb.adtype and ta.country = tb.country and ta.channel = tb.channel and ta.offer = tb.offer)" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > for_hunk.data
echo "load data local infile '/opt/ReportApi/scripts/ssp/for_hunk.data' replace into table ssp_system.huk_ssp;" | mysql -hssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -umysql -pyeahmobi
