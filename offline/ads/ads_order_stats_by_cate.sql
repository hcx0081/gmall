-- 建表
drop table if exists ads_order_stats_by_cate;
create external table ads_order_stats_by_cate
(
    dt               string comment '统计日期',
    recent_days      bigint comment '最近天数,1:最近1天,7:最近7天,30:最近30天',
    category1_id     string comment '一级品类id',
    category1_name   string comment '一级品类名称',
    category2_id     string comment '二级品类id',
    category2_name   string comment '二级品类名称',
    category3_id     string comment '三级品类id',
    category3_name   string comment '三级品类名称',
    order_count      bigint comment '下单数',
    order_user_count bigint comment '下单人数'
) comment '各品类商品下单统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_order_stats_by_cate/';


-- 装载数据（首日和每日）
insert into ads_order_stats_by_cate
select *
from ads_order_stats_by_cate
union
(select '2024-05-05',
        1,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        count(distinct order_id) order_count,
        count(distinct user_id)  order_user_count
 from (select order_id,
              user_id,
              sku_id
       from dwd_trade_order_detail_inc
       where dt = '2024-05-05') tod
          left join (select id,
                            category1_id,
                            category1_name,
                            category2_id,
                            category2_name,
                            category3_id,
                            category3_name
                     from dim_sku_full
                     where dt = '2024-05-05') sku on tod.sku_id = sku.id
 group by category1_id,
          category1_name,
          category2_id,
          category2_name,
          category3_id,
          category3_name
 union all
 select '2024-05-05',
        7,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        count(distinct order_id) order_count,
        count(distinct user_id)  order_user_count
 from (select order_id,
              user_id,
              sku_id
       from dwd_trade_order_detail_inc
       where dt >= date_sub('2024-05-05', 6)
         and dt <= '2024-05-05') tod
          left join (select id,
                            category1_id,
                            category1_name,
                            category2_id,
                            category2_name,
                            category3_id,
                            category3_name
                     from dim_sku_full
                     where dt = '2024-05-05') sku on tod.sku_id = sku.id
 group by category1_id,
          category1_name,
          category2_id,
          category2_name,
          category3_id,
          category3_name
 union all
 select '2024-05-05',
        30,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        count(distinct order_id) order_count,
        count(distinct user_id)  order_user_count
 from (select order_id,
              user_id,
              sku_id
       from dwd_trade_order_detail_inc
       where dt >= date_sub('2024-05-05', 29)
         and dt <= '2024-05-05') tod
          left join (select id,
                            category1_id,
                            category1_name,
                            category2_id,
                            category2_name,
                            category3_id,
                            category3_name
                     from dim_sku_full
                     where dt = '2024-05-05') sku on tod.sku_id = sku.id
 group by category1_id,
          category1_name,
          category2_id,
          category2_name,
          category3_id,
          category3_name)


-- 装载数据（首日和每日）（优化）
select '2024-05-05',
       1,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sum(order_count_1d),
       count(distinct user_id)
from dws_trade_user_sku_order_1d
union
select '2024-05-05',
       recent_days,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sum(order_count),
       count(distinct user_id)
from (select recent_days,
             category1_id,
             category1_name,
             category2_id,
             category2_name,
             category3_id,
             category3_name,
             if(recent_days = 7, order_count_7d, order_count_30d) order_count,
             user_id
      from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
      where dt = '2024-05-05') tmp
where order_count > 0
group by recent_days,
         category1_id,
         category1_name,
         category2_id,
         category2_name,
         category3_id,
         category3_name;