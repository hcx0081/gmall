#!/bin/bash

# 定义变量方便修改
APP=gmall

if [ -n "$1" ]; then
  do_date=$1
else
  do_date=$(date -d "-1 day" +%F)
fi

echo 日志日期: $do_date
sql="load data inpath '/${APP}/od/log/topic_log/$do_date' into table ${APP}.ods_log_inc partition(dt='$do_date');"
hive -e "$sql"
