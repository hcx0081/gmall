-- 建表
drop table if exists ads_user_action;
create external table ads_user_action
(
    dt                string comment '统计日期',
    home_count        bigint comment '浏览首页人数',
    good_detail_count bigint comment '浏览商品详情页人数',
    cart_count        bigint comment '加购人数',
    order_count       bigint comment '下单人数',
    payment_count     bigint comment '支付人数'
) comment '用户行为漏斗分析'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_user_action/';


-- 装载数据
insert overwrite table ads_user_action
select *
from ads_user_action
union
select '2024-05-05',
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
from (select 1                                      a,
             sum(if(page_id = 'home', 1, 0))        home_count,
             sum(if(page_id = 'good_detail', 1, 0)) good_detail_count
      from dws_traffic_page_visitor_page_view_1d
      where dt = '2024-05-05'
          and page_id = 'home'
         or page_id = 'good_detail') pvpv
         join (select 1              b,
                      count(user_id) cart_count
               from dws_trade_user_cart_add_1d
               where dt = '2024-05-05') cart on pvpv.a = cart.b
         join (select 1              c,
                      count(user_id) order_count
               from dws_trade_user_order_1d
               where dt = '2024-05-05') ord on cart.b = ord.c
         join (select 1              d,
                      count(user_id) payment_count
               from dws_trade_user_payment_1d
               where dt = '2024-05-05') payment on ord.c = payment.d;