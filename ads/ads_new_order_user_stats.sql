-- 建表
drop table if exists ads_new_order_user_stats;
create external table ads_new_order_user_stats
(
    dt                   string comment '统计日期',
    recent_days          bigint comment '最近天数,1:最近1天,7:最近7天,30:最近30天',
    new_order_user_count bigint comment '新增下单人数'
) comment '新增下单用户统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_new_order_user_stats/';


-- 装载数据
insert overwrite table ads_new_order_user_stats
select *
from ads_new_order_user_stats
union
select *
from (select '2024-05-05',
             count(distinct user_id),
             1
      from dwd_trade_order_detail_inc
      where dt = '2024-05-05'
        and user_id not in
            (select user_id
             from dwd_trade_order_detail_inc
             where dt < '2024-05-05')
      union all
      select '2024-05-05',
             count(distinct user_id),
             7
      from dwd_trade_order_detail_inc
      where dt >= date_sub('2024-05-05', 6)
        and dt <= '2024-05-05'
        and user_id not in
            (select user_id
             from dwd_trade_order_detail_inc
             where dt < date_sub('2024-05-05', 6))
      union all
      select '2024-05-05',
             count(distinct user_id),
             30
      from dwd_trade_order_detail_inc
      where dt >= date_sub('2024-05-05', 29)
        and dt <= '2024-05-05'
        and user_id not in
            (select user_id
             from dwd_trade_order_detail_inc
             where dt < date_sub('2024-05-05', 29))) t;


-- 装载数据（使用dws层）
insert overwrite table ads_new_order_user_stats
select *
from ads_new_order_user_stats
union
select *
from (select '2024-05-05',
             1,
             count(user_id)
      from dws_trade_user_order_td
      where dt = '2024-05-05'
        and order_date_first = '2024-05-05'
      union all
      select '2024-05-05',
             7,
             count(user_id)
      from dws_trade_user_order_td
      where dt = '2024-05-05'
        and order_date_first >= date_sub('2024-05-05', 6)
        and order_date_first <= '2024-05-05'
      union all
      select '2024-05-05',
             30,
             count(user_id)
      from dws_trade_user_order_td
      where dt = '2024-05-05'
        and order_date_first >= date_sub('2024-05-05', 29)
        and order_date_first <= '2024-05-05') t;


-- 装载数据（优化）
insert overwrite table ads_new_order_user_stats
select *
from ads_new_order_user_stats
union
select *
from (select '2024-05-05',
             recent_days,
             count(user_id)
      from dws_trade_user_order_td lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt = '2024-05-05'
        and order_date_first >= date_sub('2024-05-05', recent_days - 1)
        and order_date_first <= '2024-05-05'
      group by recent_days) t;