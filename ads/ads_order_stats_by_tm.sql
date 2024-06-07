-- 建表
drop table if exists ads_order_stats_by_tm;
create external table ads_order_stats_by_tm
(
    `dt`               string comment '统计日期',
    `recent_days`      bigint comment '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`            string comment '品牌id',
    `tm_name`          string comment '品牌名称',
    `order_count`      bigint comment '下单数',
    `order_user_count` bigint comment '下单人数'
) comment '各品牌商品下单统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_order_stats_by_tm/';


-- 装载数据（首日和每日）
insert overwrite table ads_order_stats_by_tm
select *
from ads_order_stats_by_tm
union
select *
from (select '2024-05-05',
             1,
             `tm_id`,
             `tm_name`,
             count(distinct order_id) `order_count`,
             count(distinct user_id)  `order_user_count`
      from (select order_id,
                   user_id,
                   sku_id
            from dwd_trade_order_detail_inc
            where dt = '2024-05-05') tod
               left join (select id,
                                 tm_id,
                                 tm_name
                          from dim_sku_full
                          where dt = '2024-05-05') sku on tod.sku_id = sku.id -- 因为维度表的数据是逻辑删除，所以取第一天的数据比较完整
      group by tm_id, tm_name
      union all
      select '2024-05-05',
             7,
             `tm_id`,
             `tm_name`,
             count(distinct order_id) `order_count`,
             count(distinct user_id)  `order_user_count`
      from (select order_id,
                   user_id,
                   sku_id
            from dwd_trade_order_detail_inc
            where dt >= date_sub('2024-05-05', 6)
              and dt <= '2024-05-05') tod
               left join (select id,
                                 tm_id,
                                 tm_name
                          from dim_sku_full
                          where dt = '2024-05-05') sku on tod.sku_id = sku.id
      group by tm_id, tm_name
      union all
      select '2024-05-05',
             30,
             `tm_id`,
             `tm_name`,
             count(distinct order_id) `order_count`,
             count(distinct user_id)  `order_user_count`
      from (select order_id,
                   user_id,
                   sku_id
            from dwd_trade_order_detail_inc
            where dt >= date_sub('2024-05-05', 29)
              and dt <= '2024-05-05') tod
               left join (select id,
                                 tm_id,
                                 tm_name
                          from dim_sku_full
                          where dt = '2024-05-05') sku on tod.sku_id = sku.id
      group by tm_id, tm_name) t;


/* 优化1（1d表） */
-- 建表
drop table if exists dws_order_stats_by_tm_1d;
create external table dws_order_stats_by_tm_1d
(
    `tm_id`               string comment '品牌id',
    `tm_name`             string comment '品牌名称',
    `order_count_1d`      bigint comment '下单数',
    `order_user_count_1d` bigint comment '下单人数'
) comment '各品牌商品下单统计'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
    stored as orc
    location '/gmall/warehouse/dws/dws_order_stats_by_tm_1d/'
    tblproperties ("orc.compress" = "snappy");


-- 装载数据（dws_order_stats_by_tm_1d）
insert overwrite table dws_order_stats_by_tm_1d partition (dt = '2024-05-05')
select `tm_id`,
       `tm_name`,
       count(distinct order_id) `order_count_1d`,
       count(distinct user_id)  `order_user_count_1d`
from (select order_id,
             user_id,
             sku_id
      from dwd_trade_order_detail_inc
      where dt = '2024-05-05') tod
         left join (select id,
                           tm_id,
                           tm_name
                    from dim_sku_full
                    where dt = '2024-05-05') sku on tod.sku_id = sku.id
group by tm_id, tm_name;


-- 装载数据（ads_order_stats_by_tm）
insert overwrite table ads_order_stats_by_tm
select *
from ads_order_stats_by_tm
union
select *
from (
-- 从1d表中获取最近1天的数据，保存到ads_order_stats_by_tm表中
         select '2024-05-05',
                1,
                `tm_id`,
                `tm_name`,
                `order_count_1d`,
                `order_user_count_1d`
         from dws_order_stats_by_tm_1d
         where dt = '2024-05-05'
         union all
-- 从1d表中获取最近7天的数据，保存到ads_order_stats_by_tm表中
         select '2024-05-05',
                7,
                `tm_id`,
                `tm_name`,
                sum(`order_count_1d`),
                sum(`order_user_count_1d`)
         from dws_order_stats_by_tm_1d
         where dt >= date_sub('2024-05-05', 6)
           and dt <= '2024-05-05'
         group by tm_id, tm_name
         union all
-- 从1d表中获取最近30天的数据，保存到ads_order_stats_by_tm表中
         select '2024-05-05',
                30,
                `tm_id`,
                `tm_name`,
                sum(`order_count_1d`),
                sum(`order_user_count_1d`)
         from dws_order_stats_by_tm_1d
         where dt >= date_sub('2024-05-05', 29)
           and dt <= '2024-05-05'
         group by tm_id, tm_name) t;


-- 优化装载数据（ads_order_stats_by_tm）（使用explode函数）
insert overwrite table ads_order_stats_by_tm
select *
from ads_order_stats_by_tm
union
select '2024-05-05',
       days,
       `tm_id`,
       `tm_name`,
       sum(`order_count_1d`),
       sum(`order_user_count_1d`)
from dws_order_stats_by_tm_1d lateral view explode(array(1, 7, 30)) tmp as days
where dt >= date_sub('2024-05-05', days - 1)
  and dt <= '2024-05-05'
group by days, tm_id, tm_name;


/* 优化（nd表） */
-- 建表
drop table if exists dws_order_stats_by_tm_nd;
create external table dws_order_stats_by_tm_nd
(
    `tm_id`                string comment '品牌id',
    `tm_name`              string comment '品牌名称',
    `order_count_7d`       bigint comment '下单数',
    `order_user_count_7d`  bigint comment '下单人数',
    `order_count_30d`      bigint comment '下单数',
    `order_user_count_30d` bigint comment '下单人数'
) comment '各品牌商品下单统计'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
    stored as orc
    location '/gmall/warehouse/dws/dws_order_stats_by_tm_nd/'
    tblproperties ("orc.compress" = "snappy");


-- 装载数据（dws_order_stats_by_tm_nd）
insert overwrite table dws_order_stats_by_tm_nd partition (dt = '2024-05-05')
select `tm_id`,
       `tm_name`,
       sum(`if`(dt >= date_sub('2024-05-05', 6), order_count_1d, 0))      `order_count_7d`,
       sum(`if`(dt >= date_sub('2024-05-05', 6), order_user_count_1d, 0)) `order_user_count_7d`,
       sum(order_count_1d)                                                `order_count_30d`,
       sum(order_user_count_1d)                                           `order_user_count_30d`
from dws_order_stats_by_tm_1d
where dt >= date_sub('2024-05-05', 29)
  and dt <= '2024-05-05'
group by tm_id, tm_name;


-- 装载数据（ads_order_stats_by_tm）
insert overwrite table ads_order_stats_by_tm
select *
from ads_order_stats_by_tm
union
select *
from (select '2024-05-05',
             1,
             `tm_id`,
             `tm_name`,
             `order_count_1d`,
             `order_user_count_1d`
      from dws_order_stats_by_tm_1d
      where dt = '2024-05-05'
      union all
      select '2024-05-05',
             7,
             `tm_id`,
             `tm_name`,
             `order_count_7d`,
             `order_user_count_7d`
      from dws_order_stats_by_tm_nd
      where dt = '2024-05-05'
      union all
      select '2024-05-05',
             30,
             `tm_id`,
             `tm_name`,
             `order_count_30d`,
             `order_user_count_30d`
      from dws_order_stats_by_tm_nd
      where dt = '2024-05-05') t