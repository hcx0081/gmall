drop table if exists ods_base_dic_full;
create external table ods_base_dic_full
(
    `dic_code`     string comment '编号',
    `dic_name`     string comment '编码名称',
    `parent_code`  string comment '父编号',
    `create_time`  string comment '创建日期',
    `operate_time` string comment '修改日期'
) comment '编码字典表'
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/gmall/warehouse/ods/ods_base_dic_full/'
    tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');