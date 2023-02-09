export PGPASSWORD=rZtnJ8C4bbND
if [ -n "$1" ]; then
    start_date=$1"00"
    end_date=$1"23"
    today=$1
else
    start_date=$(date -d "1 days ago" "+%Y%m%d00")
    end_date=$(date -d "1 days ago" "+%Y%m%d23")
    today=$(date -d "1 days ago" "+%Y%m%d")
fi
echo "delete from new_ssp.cloudmobi_click_redirect_middle" | mysql -hphp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -udb_app -pVee8lie3aiNee9sa
echo "select channel,offer,domain_tag from (SELECT channel,offer,domain_tag,ROW_NUMBER () OVER ( PARTITION BY channel, offer ORDER BY count DESC ) AS row_num FROM (SELECT channel,offer,domain_tag,sum(count) as count FROM redirect WHERE created >= current_timestamp - interval '12 hour' group by channel,offer,domain_tag) a group by channel,offer,domain_tag,count )b where row_num=1"| psql -h log-alb.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user root --db hdtlog -F "	" -t -A > click_redirect.data
echo "load data local infile '/opt/ReportApi/scripts/ssp/click_redirect.data' replace into table new_ssp.cloudmobi_click_redirect_middle;" | mysql -hphp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -udb_app -pVee8lie3aiNee9sa
echo "replace into new_ssp.cloudmobi_click_redirect select channel,offer_id,domain_tag,now() from (SELECT channel,offer_id,case  when b is null then a when locate(a,b)=0 then a ELSE null END as domain_tag FROM (SELECT middle.channel as channel,middle.offer_id as offer_id,middle.domain_tag AS a,final.domain_tag AS b FROM new_ssp.cloudmobi_click_redirect_middle AS middle LEFT JOIN new_ssp.cloudmobi_click_redirect AS final ON middle.channel = final.channel AND middle.offer_id = final.offer_id ) a)b where domain_tag is not null" | mysql -hphp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -udb_app -pVee8lie3aiNee9sa
#echo "SELECT channel,offer,domain_tag,created FROM( SELECT channel, offer, domain_tag, created, ROW_NUMBER () OVER ( PARTITION BY channel, offer, domain_tag ORDER BY created DESC ) AS row_num FROM redirect where time_slot>='${start_date}')a WHERE row_num = 1" | psql -h log-alb.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com -p 5439 --user root --db hdtlog -F "	" -t -A > click_redirect.data
#echo "load data local infile '/opt/ReportApi/scripts/ssp/click_redirect.data' replace into table new_ssp.cloudmobi_click_redirect_middle;" | mysql -hphp-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com -udb_app -pVee8lie3aiNee9sa
