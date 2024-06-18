-- 建表
drop table if exists dim_promotion_pos_full;
create external table dim_promotion_pos_full
(
    `id`             string comment '营销坑位id',
    `pos_location`   string comment '营销坑位位置',
    `pos_type`       string comment '营销坑位类型 ',
    `promotion_type` string comment '营销类型',
    `create_time`    string comment '创建时间',
    `operate_time`   string comment '修改时间'
) comment '营销坑位维度表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dim/dim_promotion_pos_full/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dim_promotion_pos_full partition (dt = '2024-05-05')
select `id`,
       `pos_location`,
       `pos_type`,
       `promotion_type`,
       `create_time`,
       `operate_time`
from ods_promotion_pos_full
where dt = '2024-05-05';