-- 建表
drop table if exists dwd_interaction_favor_add_inc;
create external table dwd_interaction_favor_add_inc
(
    `id`          string comment '编号',
    `user_id`     string comment '用户id',
    `sku_id`      string comment 'sku_id',
    `date_id`     string comment '日期id',
    `create_time` string comment '收藏时间'
) comment '互动域收藏商品事务事实表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_interaction_favor_add_inc/'
    tblproperties ("orc.compress" = "snappy");


-- 装载数据（首日）
insert overwrite table dwd_interaction_favor_add_inc partition (dt)
select data.`id`,
       data.`user_id`,
       data.`sku_id`,
       date_format(data.`create_time`, 'yyyy-MM-dd') `date_id`,
       data.`create_time`,
       date_format(data.`create_time`, 'yyyy-MM-dd')
from ods_favor_info_inc
where dt = '2024-05-05'
  and type = 'bootstrap-insert';


-- 装载数据（每日）
insert overwrite table dwd_interaction_favor_add_inc partition (dt = '2024-05-06')
select data.`id`,
       data.`user_id`,
       data.`sku_id`,
       date_format(data.`create_time`, 'yyyy-MM-dd') `date_id`,
       data.`create_time`
from ods_favor_info_inc
where dt = '2024-05-06'
  and type = 'insert';