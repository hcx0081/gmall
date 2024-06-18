#!/bin/bash
APP=gmall

if [ -n "$2" ]; then
  do_date=$2
else
  echo "请传入日期参数"
  exit
fi

dws_trade_user_order_td="
insert overwrite table ${APP}.dws_trade_user_order_td partition (dt = '$do_date')
select user_id,
       min(dt)                        order_date_first,
       max(dt)                        order_date_last,
       sum(order_count_1d)            order_count_td,
       sum(order_num_1d)              order_num_td,
       sum(order_original_amount_1d)  original_amount_td,
       sum(activity_reduce_amount_1d) activity_reduce_amount_td,
       sum(coupon_reduce_amount_1d)   coupon_reduce_amount_td,
       sum(order_total_amount_1d)     total_amount_td
from ${APP}.dws_trade_user_order_1d
group by user_id;
"

dws_user_user_login_td="
insert overwrite table ${APP}.dws_user_user_login_td partition (dt = '$do_date')
select user_id,
       max(dt)  login_date_last,
       min(dt)  login_date_first,
       count(*) login_count_td
from ${APP}.dwd_user_login_inc
group by user_id;
"

case $1 in
"dws_trade_user_order_td")
  hive -e "$dws_trade_user_order_td"
  ;;
"dws_user_user_login_td")
  hive -e "$dws_user_user_login_td"
  ;;
"all")
  hive -e "$dws_trade_user_order_td$dws_user_user_login_td"
  ;;
esac
