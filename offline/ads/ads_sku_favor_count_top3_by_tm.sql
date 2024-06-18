-- 建表
drop table if exists ads_sku_favor_count_top3_by_tm;
create external table ads_sku_favor_count_top3_by_tm
(
    dt          string comment '统计日期',
    tm_id       string comment '品牌id',
    tm_name     string comment '品牌名称',
    sku_id      string comment 'sku_id',
    sku_name    string comment 'sku名称',
    favor_count bigint comment '被收藏次数',
    rk          bigint comment '排名'
) comment '各品牌商品收藏次数top3'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_sku_favor_count_top3_by_tm/';


-- 装载数据
select '2024-05-05',
       tm_id,
       tm_name,
       sku_id,
       sku_name,
       favor_count,
       rk
from (select tm_id,
             tm_name,
             sku_id,
             sku_name,
             favor_add_count_1d                                                       favor_count,
             rank() over (partition by tm_id,sku_id order by favor_add_count_1d desc) rk
      from dws_interaction_sku_favor_add_1d
      where dt = '2024-05-05'
      group by tm_id, tm_name, sku_id, sku_name) t
where rk <= 3;