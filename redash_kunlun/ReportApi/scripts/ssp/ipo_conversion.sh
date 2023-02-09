export PGPASSWORD=Cloudtech2018
if [ -n "$1" ]; then
    start_date=$1"00"
    end_date=$1"23"
else
    start_date=$(date -d "1 days ago" "+%Y%m%d00")
    end_date=$(date -d "1 days ago" "+%Y%m%d23")
fi
echo $start_date
echo "select substring(time_slot,0,9),country,channel,offer, slot,  sum(count) as conv, sum(payout) as payout from conversion where time_slot>=$start_date and time_slot<=$end_date and Len(country)<=2 group by substring(time_slot,0,9),country,channel,offer,slot" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > ipo_conversion.conv
echo "load data local infile '/opt/ReportApi/scripts/ssp/ipo_conversion.conv' replace into table new_ssp.ipo_map_offer_slot;" | mysql -hphp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -udb_app -pVee8lie3aiNee9sa

