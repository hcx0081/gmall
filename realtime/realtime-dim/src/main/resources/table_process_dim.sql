# 注意区分gmall_config配置库和gmall业务库
create database gmall_config charset utf8mb4 default collate utf8mb4_general_ci;

use gmall_config;

drop table if exists table_process_dim;
create table table_process_dim
(
    source_table varchar(200) not null comment '来源表名',
    sink_table   varchar(200) not null comment '输出表名',
    sink_family  varchar(200) comment '输出到HBase的列族',
    sink_columns varchar(2000) comment '输出字段',
    sink_row_key varchar(200) comment '输出到HBase的的RowKey',
    primary key (sink_table)
) engine = innodb
  default charset = utf8;

insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('activity_info', 'dim_activity_info', 'info',
        'id,activity_name,activity_type,activity_desc,start_time,end_time,create_time', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('activity_rule', 'dim_activity_rule', 'info',
        'id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level',
        'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('activity_sku', 'dim_activity_sku', 'info', 'id,activity_id,sku_id,create_time', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('base_category1', 'dim_base_category1', 'info', 'id,name', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('base_category2', 'dim_base_category2', 'info', 'id,name,category1_id', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('base_category3', 'dim_base_category3', 'info', 'id,name,category2_id', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('base_province', 'dim_base_province', 'info', 'id,name,region_id,area_code,iso_code,iso_3166_2', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('base_region', 'dim_base_region', 'info', 'id,region_name', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('base_trademark', 'dim_base_trademark', 'info', 'id,tm_name', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('coupon_info', 'dim_coupon_info', 'info',
        'id,coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc',
        'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('coupon_range', 'dim_coupon_range', 'info', 'id,coupon_id,range_type,range_id', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('financial_sku_cost', 'dim_financial_sku_cost', 'info',
        'id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('sku_info', 'dim_sku_info', 'info',
        'id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('spu_info', 'dim_spu_info', 'info', 'id,spu_name,description,category3_id,tm_id', 'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('user_info', 'dim_user_info', 'info', 'id,login_name,name,user_level,birthday,gender,create_time,operate_time',
        'id');
insert into table_process_dim(source_table, sink_table, sink_family, sink_columns, sink_row_key)
values ('base_dic', 'dim_base_dic', 'info', 'dic_code,dic_name', 'dic_code');