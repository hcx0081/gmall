-- 建表
drop table if exists ads_user_retention;
create external table ads_user_retention
(
    dt              string comment '统计日期',
    create_date     string comment '用户新增日期',
    retention_day   int comment '截至当前日期留存天数',
    retention_count bigint comment '留存用户数量',
    new_user_count  bigint comment '新增用户数量',
    retention_rate  decimal(16, 2) comment '留存率'
) comment '用户留存率'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_user_retention/';


-- 装载数据
insert overwrite table ads_user_retention
select *
from ads_user_retention
union
select '2024-05-05',
       login_date_first,
       datediff('2024-05-05', login_date_first),
       sum(if(login_date_last = '2024-05-05', 1, 0))                  retention_count,
       count(*)                                                       user_new_count,
       sum(if(login_date_last = '2024-05-05', 1, 0)) / count(*) * 100 new_user_count
from dws_user_user_login_td
where dt = '2024-05-05'
  and login_date_first >= date_sub('2024-05-05', 7)
  and login_date_first < '2024-05-05'
group by login_date_first;