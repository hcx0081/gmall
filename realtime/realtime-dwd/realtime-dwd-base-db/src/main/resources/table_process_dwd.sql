use gmall_config;

drop table if exists `table_process_dwd`;
create table `table_process_dwd`
(
    `source_table` varchar(200) not null comment '来源表名',
    `source_type`  varchar(200) not null comment '来源操作类型',
    `sink_table`   varchar(200) not null comment '目标表名',
    `sink_columns` varchar(2000) comment '输出字段',
    primary key (`sink_table`)
) engine = innodb
  default charset = utf8;

insert into `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`)
values ('coupon_use', 'insert', 'dwd_tool_coupon_get', 'id,coupon_id,user_id,get_time,coupon_status');
insert into `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`)
values ('coupon_use', 'update', 'dwd_tool_coupon_use',
        'id,coupon_id,user_id,order_id,using_time,used_time,coupon_status');
insert into `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`)
values ('favor_info', 'insert', 'dwd_interaction_favor_add', 'id,user_id,sku_id,create_time');
insert into `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`)
values ('user_info', 'insert', 'dwd_user_register', 'id,create_time');