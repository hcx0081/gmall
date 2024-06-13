-- 建表
drop table if exists dws_interaction_sku_favor_add_1d;
create external table dws_interaction_sku_favor_add_1d
(
    sku_id             string comment 'sku_id',
    sku_name           string comment 'sku名称',
    category1_id       string comment '一级品类id',
    category1_name     string comment '一级品类名称',
    category2_id       string comment '二级品类id',
    category2_name     string comment '二级品类名称',
    category3_id       string comment '三级品类id',
    category3_name     string comment '三级品类名称',
    tm_id              string comment '品牌id',
    tm_name            string comment '品牌名称',
    favor_add_count_1d bigint comment '商品被收藏次数'
) comment '互动域商品粒度收藏商品最近1日汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_interaction_sku_favor_add_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dws_interaction_sku_favor_add_1d partition (dt)
select sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       favor_add_count_1d,
       dt
from (select sku_id,
             count(*) favor_add_count_1d,
             dt
      from dwd_interaction_favor_add_inc
      group by dt, sku_id) ifa
         left join (select id,
                           sku_name,
                           category1_id,
                           category1_name,
                           category2_id,
                           category2_name,
                           category3_id,
                           category3_name,
                           tm_id,
                           tm_name
                    from dim_sku_full
                    where dt = '2024-05-05') sku on ifa.sku_id = sku.id;


-- 装载数据（每日）
insert overwrite table dws_interaction_sku_favor_add_1d partition (dt = '2024-05-06')
select sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       favor_add_count_1d
from (select sku_id,
             count(*) favor_add_count_1d
      from dwd_interaction_favor_add_inc
      group by sku_id) ifa
         left join (select id,
                           sku_name,
                           category1_id,
                           category1_name,
                           category2_id,
                           category2_name,
                           category3_id,
                           category3_name,
                           tm_id,
                           tm_name
                    from dim_sku_full
                    where dt = '2024-05-06') sku on ifa.sku_id = sku.id;