-- 建表
drop table if exists dim_promotion_refer_full;
create external table dim_promotion_refer_full
(
    `id`           string comment '营销渠道id',
    `refer_name`   string comment '营销渠道名称',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '修改时间'
) comment '营销渠道维度表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dim/dim_promotion_refer_full/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dim_promotion_refer_full partition (dt = '2024-05-05')
select `id`,
       `refer_name`,
       `create_time`,
       `operate_time`
from ods_promotion_refer_full
where dt = '2024-05-05';