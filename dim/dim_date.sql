-- 建表
drop table if exists dim_date;
create external table dim_date
(
    `date_id`    string comment '日期id',
    `week_id`    string comment '周id,一年中的第几周',
    `week_day`   string comment '周几',
    `day`        string comment '每月的第几天',
    `month`      string comment '一年中的第几月',
    `quarter`    string comment '一年中的第几季度',
    `year`       string comment '年份',
    `is_workday` string comment '是否是工作日',
    `holiday_id` string comment '节假日'
) comment '日期维度表'
    stored as orc
    location '/gmall/warehouse/dim/dim_date/'
    tblproperties ('orc.compress' = 'snappy');


-- 创建临时表
drop table if exists tmp_dim_date_info;
create external table tmp_dim_date_info
(
    `date_id`    string comment '日',
    `week_id`    string comment '周id',
    `week_day`   string comment '周几',
    `day`        string comment '每月的第几天',
    `month`      string comment '第几月',
    `quarter`    string comment '第几季度',
    `year`       string comment '年',
    `is_workday` string comment '是否是工作日',
    `holiday_id` string comment '节假日'
) comment '时间维度表'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/tmp/tmp_dim_date_info/';


-- 装载数据
insert overwrite table dim_date
select *
from tmp_dim_date_info;