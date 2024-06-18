-- 建表
drop table if exists ads_sku_cart_num_top3_by_cate;
create external table ads_sku_cart_num_top3_by_cate
(
    dt             string comment '统计日期',
    category1_id   string comment '一级品类id',
    category1_name string comment '一级品类名称',
    category2_id   string comment '二级品类id',
    category2_name string comment '二级品类名称',
    category3_id   string comment '三级品类id',
    category3_name string comment '三级品类名称',
    sku_id         string comment 'sku_id',
    sku_name       string comment 'sku名称',
    cart_num       bigint comment '购物车中商品数量',
    rk             bigint comment '排名'
) comment '各品类商品购物车存量top3'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_sku_cart_num_top3_by_cate/';


-- 装载数据
insert overwrite table ads_sku_cart_num_top3_by_cate
select *
from ads_sku_cart_num_top3_by_cate
union
select '2024-05-05',
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sku_id,
       sku_name,
       cart_num,
       rk
from (select category1_id,
             category1_name,
             category2_id,
             category2_name,
             category3_id,
             category3_name,
             sku_id,
             sku_name,
             cart_num,
             rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc ) rk
      from (select category1_id,
                   category1_name,
                   category2_id,
                   category2_name,
                   category3_id,
                   category3_name,
                   sku_id,
                   sku_name,
                   sum(sku_num) cart_num
            from (select sku_id,
                         sku_name,
                         sku_num
                  from dwd_trade_cart_full
                  where dt = '2024-05-05') cart
                     left join (select id,
                                       category1_id,
                                       category1_name,
                                       category2_id,
                                       category2_name,
                                       category3_id,
                                       category3_name
                                from dim_sku_full
                                where dt = '2024-05-05') sku on cart.sku_id = sku.id
            group by category1_id,
                     category1_name,
                     category2_id,
                     category2_name,
                     category3_id,
                     category3_name) tmp) t
where rk <= 3;