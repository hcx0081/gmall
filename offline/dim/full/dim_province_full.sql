-- 建表
drop table if exists dim_province_full;
create external table dim_province_full
(
    `id`            string comment '省份id',
    `province_name` string comment '省份名称',
    `area_code`     string comment '地区编码',
    `iso_code`      string comment '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2`    string comment '新版国际标准地区编码，供可视化使用',
    `region_id`     string comment '地区id',
    `region_name`   string comment '地区名称'
) comment '地区维度表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dim/dim_province_full/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据
insert overwrite table dim_province_full partition (dt = '2024-05-05')
select province.id,
       province.name,
       province.area_code,
       province.iso_code,
       province.iso_3166_2,
       region_id,
       region_name
from (select id,
             name,
             region_id,
             area_code,
             iso_code,
             iso_3166_2
      from ods_base_province_full
      where dt = '2024-05-05') province
         left join
     (select id,
             region_name
      from ods_base_region_full
      where dt = '2024-05-05') region
     on province.region_id = region.id;