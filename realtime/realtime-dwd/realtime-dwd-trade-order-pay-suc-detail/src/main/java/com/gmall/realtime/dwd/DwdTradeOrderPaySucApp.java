package com.gmall.realtime.dwd;

import com.gmall.realtime.common.base.BaseSQLApp;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.FlinkSQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderPaySucApp extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeOrderPaySucApp().start(10016, 4, "dwd-trade-order-pay-suc-app");
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        /* 必须设置超时时长！！！ */
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        
        createTopicDbSourceFromKafka(tEnv);
        
        Table paymentInfo = selectPaymentInfo(tEnv);
        tEnv.createTemporaryView("payment_info", paymentInfo);
        
        createOrderDetailSourceFromKafka(tEnv);
        
        Table paymentOrder = selectPaymentOrder(tEnv);
        tEnv.createTemporaryView("payment_order", paymentOrder);
        
        createBaseDicSourceFromHBase(tEnv);
        
        Table joinTable = tEnv.sqlQuery("SELECT " +
                "       id,\n" +
                "       order_id,\n" +
                "       user_id,\n" +
                "       payment_type  payment_type_code,\n" +
                "       info.dic_name payment_type_name,\n" +
                "       sku_id,\n" +
                "       sku_name,\n" +
                "       order_price,\n" +
                "       split_total_amount,\n" +
                "       split_activity_amount,\n" +
                "       split_coupon_amount,\n" +
                "       province_id,\n" +
                "       activity_id,\n" +
                "       activity_rule_id,\n" +
                "       coupon_id,\n" +
                "       ts\n" +
                "FROM payment_order as po\n" +
                "         left join base_dic_source FOR SYSTEM_TIME AS OF po.proc_time as bd\n" +
                "on po.payment_type = bd.rowkey");
        
        createTopicDwdTradeOrderPaymentSuccessSinkToKafka(tEnv);
        joinTable.insertInto("topic_dwd_trade_order_payment_success_sink").execute();
    }
    
    private Table selectPaymentInfo(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "select " +
                        "       `data`['id']            id,\n" +
                        "       `data`['order_id']      order_id,\n" +
                        "       `data`['user_id']       user_id,\n" +
                        "       `data`['payment_type']  payment_type,\n" +
                        "       `data`['total_amount']  total_amount,\n" +
                        "       `data`['callback_time'] callback_time,\n" +
                        "       ts,\n" +
                        
                        "       event_time,\n" +
                        "       proc_time\n" +
                        "from topic_db_source\n" +
                        "where `database` = 'gmall'\n" +
                        "  and `table` = 'payment_info'\n" +
                        "  and `type` = 'update'\n" +
                        "  and `old`['payment_status'] is not null\n" +
                        "  and `data`['payment_status'] = '1602'"
        );
    }
    
    private void createOrderDetailSourceFromKafka(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE order_detail_source\n" +
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
                
                "    `event_time` as TO_TIMESTAMP_LTZ(`ts`, 0),\n" +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '4' SECOND\n" +
                ")" + FlinkSQLUtils.withSQLFromKafka(Constants.TOPIC_DWD_TRADE_ORDER_DETAIL, Constants.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }
    
    private Table selectPaymentOrder(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("SELECT od.id,\n" +
                "       pi.order_id,\n" +
                "       pi.user_id,\n" +
                "       payment_type,\n" +
                "       callback_time payment_time,\n" +
                "       sku_id,\n" +
                "       sku_name,\n" +
                "       order_price,\n" +
                "       split_total_amount,\n" +
                "       split_activity_amount,\n" +
                "       split_coupon_amount,\n" +
                "       province_id,\n" +
                "       activity_id,\n" +
                "       activity_rule_id,\n" +
                "       coupon_id,\n" +
                "       pi.ts,\n" +
                
                "       proc_time\n" +
                "FROM payment_info pi\n" +
                "         left join order_detail_source od on pi.order_id = od.order_id\n" +
                "where od.event_time BETWEEN od.event_time - INTERVAL '15' MINUTE AND od.event_time + INTERVAL '5' SECOND");
    }
    
    private void createTopicDwdTradeOrderPaymentSuccessSinkToKafka(StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table topic_dwd_trade_order_payment_success_sink\n" +
                "(\n" +
                "    id                    string,\n" +
                "    order_id              string,\n" +
                "    user_id               string,\n" +
                "    payment_type_code     string,\n" +
                "    payment_type_name     string,\n" +
                "    sku_id                string,\n" +
                "    sku_name              string,\n" +
                "    order_price           string,\n" +
                "    split_total_amount    string,\n" +
                "    split_activity_amount string,\n" +
                "    split_coupon_amount   string,\n" +
                "    province_id           string,\n" +
                "    activity_id           string,\n" +
                "    activity_rule_id      string,\n" +
                "    coupon_id             string,\n" +
                "    ts                    bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED" +
                ")" + FlinkSQLUtils.withSQLToUpsertKafka(Constants.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }
}
