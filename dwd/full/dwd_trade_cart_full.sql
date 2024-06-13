-- 建表
drop table if exists dwd_trade_cart_full;
create external table dwd_trade_cart_full
(
    id       string comment '编号',
    user_id  string comment '用户id',
    sku_id   string comment 'sku_id',
    sku_name string comment '商品名称',
    sku_num  bigint comment '现存商品件数'
) comment '交易域购物车周期快照事实表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_trade_cart_full/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dwd_trade_cart_full partition (dt = '2024-05-05')
select id,
       user_id,
       sku_id,
       sku_name,
       sku_num
from ods_cart_info_full
where dt = '2024-05-05'
  and is_ordered = '0';