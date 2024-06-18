-- 建表
drop table if exists dws_order_stats_by_sku_1d;
create external table dws_order_stats_by_sku_1d
(
    tm_id          string comment '品牌id',
    tm_name        string comment '品牌名称',
    category1_id   string comment '一级品类id',
    category1_name string comment '一级品类名称',
    category2_id   string comment '二级品类id',
    category2_name string comment '二级品类名称',
    category3_id   string comment '三级品类id',
    category3_name string comment '三级品类名称',
    order_count_1d bigint comment '下单数',
    user_id        bigint comment '下单用户id'
) comment '各sku商品下单统计'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as orc
    location '/gmall/warehouse/dws/dws_order_stats_by_sku_1d/'
    tblproperties ("orc.compress" = "snappy");