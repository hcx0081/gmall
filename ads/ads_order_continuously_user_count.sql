-- 建表
drop table if exists ads_order_continuously_user_count;
create external table ads_order_continuously_user_count
(
    `dt`                            string comment '统计日期',
    `recent_days`                   bigint comment '最近天数,7:最近7天',
    `order_continuously_user_count` bigint comment '连续3日下单用户数'
) comment '最近7日内连续3日下单用户数统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_order_continuously_user_count/';


-- 装载数据
/*
    - 连续3日的基本条件：数据数量大于等于3
    - 连续的第三条数据的时间减去当前时间应该为2
    - 一个用户可能会重复出现连续3天，所以统计时需要去重
*/
select '2024-05-05',
       7,
       count(distinct user_id)
from (select user_id,
             datediff(lead(dt, 2, '9999-12-31') over (partition by user_id order by dt), dt) diff
      from dws_trade_user_order_1d
      where dt >= date_sub('2024-05-05', 6)
        and dt <= '2024-05-05') t
where diff = 2
