export PGPASSWORD=Cloudtech2018
echo "$(date) begin" > log
today=$(date -d "7 days ago" "+%Y%m%d00")
echo "select  slot,  sum(count) c, time_slot, country, sum(filled) from request where time_slot>=$today and visible=1 group by time_slot, slot, country" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > data.req
echo "truncate table ssp_system.request_ssp;commit;load data local infile '/opt/ReportApi/scripts/ssp/data.req' into table ssp_system.request_ssp;" | mysql -hssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -umysql -pyeahmobi

# add sls_request
echo "select  slot,  sum(count) c, time_slot, country, sum(filled) from request where time_slot>=$today and adtype=3 group by time_slot, slot, country" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > data_sls.req
echo "truncate table ssp_system.request_sls;commit;load data local infile '/opt/ReportApi/scripts/ssp/data_sls.req' into table ssp_system.request_sls;" | mysql -hssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -umysql -pyeahmobi


echo "select  slot,  sum(count) c, time_slot, country  from impression where time_slot>=$today and visible=1 and adtype!=12 group by time_slot, slot, country" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > data.imp
echo "truncate table ssp_system.impression_ssp;load data local infile '/opt/ReportApi/scripts/ssp/data.imp' into table ssp_system.impression_ssp;" | mysql -hssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -umysql -pyeahmobi

echo "select  slot,  sum(count) c, time_slot, country  from click where time_slot>=$today group by time_slot, slot, country" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > data.click
echo "truncate table ssp_system.click_ssp;load data local infile '/opt/ReportApi/scripts/ssp/data.click' into table ssp_system.click_ssp;" | mysql -hssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -umysql -pyeahmobi
# add all_click
echo "select  slot,  sum(count) c, time_slot, country  from impression where time_slot>=$today and visible=0 group by time_slot, slot, country" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > data_all.click
echo "truncate table ssp_system.click_all;load data local infile '/opt/ReportApi/scripts/ssp/data_all.click' into table ssp_system.click_all;" | mysql -hssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -umysql -pyeahmobi


echo "select  slot,  sum(count) c, sum(payout) p, time_slot, country from conversion where time_slot>=$today group by time_slot, slot, country" | psql -h cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user cloudtech --db monitor -F "	" -t -A > data.conv
echo "truncate table ssp_system.conversion_ssp;load data local infile '/opt/ReportApi/scripts/ssp/data.conv' into table ssp_system.conversion_ssp;" | mysql -hssp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -umysql -pyeahmobi
echo "$(date) end" >> log
