package com.gmall.realtime.dwd;

import com.gmall.realtime.common.base.BaseSQLApp;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.FlinkSQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetailApp extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeOrderDetailApp().start(10014, 4, "dwd-trade-order-detail-app");
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        /* 必须设置超时时长！！！ */
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        
        createTopicDbSourceFromKafka(tEnv);
        
        // order_detail、order_info、order_detail_activity、order_detail_coupon
        
        Table orderDetail = selectOrderDetail(tEnv);
        tEnv.createTemporaryView("order_detail", orderDetail);
        
        Table orderInfo = selectOrderInfo(tEnv);
        tEnv.createTemporaryView("order_info", orderInfo);
        
        Table orderDetailActivity = selectOrderDetailActivity(tEnv);
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
        
        Table orderDetailCoupon = selectOrderDetailCoupon(tEnv);
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        
        Table joinTable = tEnv.sqlQuery(
                "select " +
                        "       od.id,\n" +
                        "       order_id,\n" +
                        "       sku_id,\n" +
                        "       sku_name,\n" +
                        "       order_price,\n" +
                        "       create_time,\n" +
                        "       split_total_amount,\n" +
                        "       split_activity_amount,\n" +
                        "       split_coupon_amount,\n" +
                        "       user_id,\n" +
                        "       province_id,\n" +
                        "       activity_id,\n" +
                        "       activity_rule_id,\n" +
                        "       coupon_id,\n" +
                        "       od.ts\n" +
                        "from order_detail od\n" +
                        "         join order_info oi on od.order_id = oi.id\n" +
                        "         left join order_detail_activity oda on oda.order_detail_id = od.id\n" +
                        "         left join order_detail_coupon odc on oda.order_detail_id = od.id"
        );
        
        createTopicDwdTradeOrderDetailSinkToKafka(tEnv);
        
        joinTable.insertInto(Constants.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }
    
    
    private Table selectOrderDetail(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "select " +
                        "       data['id']                      id,\n" +
                        "       data['order_id']                order_id,\n" +
                        "       data['sku_id']                  sku_id,\n" +
                        "       data['sku_name']                sku_name,\n" +
                        "       data['order_price']             order_price,\n" +
                        "       data['create_time']             create_time,\n" +
                        "       data['split_total_amount']      split_total_amount,\n" +
                        "       data['split_activity_amount']   split_activity_amount,\n" +
                        "       data['split_coupon_amount']     split_coupon_amount,\n" +
                        "       ts\n" +
                        "from topic_db_source\n" +
                        "where `database` = 'gmall'\n" +
                        "  and `table` = 'order_detail'\n" +
                        "  and `type` = 'insert'"
        );
    }
    
    private Table selectOrderInfo(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "select " +
                        "       data['id']              id,\n" +
                        "       data['user_id']         user_id,\n" +
                        "       data['province_id']     province_id,\n" +
                        "       ts\n" +
                        "from topic_db_source\n" +
                        "where `database` = 'gmall'\n" +
                        "  and `table` = 'order_info'\n" +
                        "  and `type` = 'insert'"
        );
    }
    
    private Table selectOrderDetailActivity(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "select " +
                        "       data['order_detail_id']     order_detail_id,\n" +
                        "       data['activity_id']         activity_id,\n" +
                        "       data['activity_rule_id']    activity_rule_id,\n" +
                        "       ts\n" +
                        "from topic_db_source\n" +
                        "where `database` = 'gmall'\n" +
                        "  and `table` = 'order_detail_activity'\n" +
                        "  and `type` = 'insert'"
        );
    }
    
    private Table selectOrderDetailCoupon(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "select " +
                        "       data['order_detail_id']     order_detail_id,\n" +
                        "       data['coupon_id']           coupon_id,\n" +
                        "       ts\n" +
                        "from topic_db_source\n" +
                        "where `database` = 'gmall'\n" +
                        "  and `table` = 'order_detail_coupon'\n" +
                        "  and `type` = 'insert'"
        );
    }
    
    public void createTopicDwdTradeOrderDetailSinkToKafka(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE topic_dwd_trade_order_detail_sink\n" +
                "(\n" +
                "    id                    string,\n" +
                "    order_id              string,\n" +
                "    sku_id                string,\n" +
                "    sku_name              string,\n" +
                "    order_price           string,\n" +
                "    create_time           string,\n" +
                "    split_total_amount    string,\n" +
                "    split_activity_amount string,\n" +
                "    split_coupon_amount   string,\n" +
                "    user_id               string,\n" +
                "    province_id           string,\n" +
                "    activity_id           string,\n" +
                "    activity_rule_id      string,\n" +
                "    coupon_id             string,\n" +
                "    ts                    bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED" +
                ")" + FlinkSQLUtils.withSQLToUpsertKafka(Constants.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }
}
