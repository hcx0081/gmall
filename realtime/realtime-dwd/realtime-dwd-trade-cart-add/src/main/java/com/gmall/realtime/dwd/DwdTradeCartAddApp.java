package com.gmall.realtime.dwd;

import com.gmall.realtime.common.base.BaseSQLApp;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.FlinkSQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易域加购事务事实表
 */
public class DwdTradeCartAddApp extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeCartAddApp().start(10013, 4, "dwd-trade-cart-add-app");
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        createTopicDbSourceFromKafka(tEnv);
        
        Table cartInfo = selectCartInfo(tEnv);
        
        createTopicDwdTradeCartAddToKafka(tEnv);
        
        cartInfo.insertInto("topic_dwd_trade_cart_add_sink").execute();
    }
    
    private Table selectCartInfo(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "select " +
                        "       data['id']                                                                                                                          id,\n" +
                        "       data['user_id']                                                                                                                     user_id,\n" +
                        "       data['sku_id']                                                                                                                      sku_id,\n" +
                        "       data['cart_price']                                                                                                                  cart_price,\n" +
                        "       if(`type` = 'insert', cast(`data`['sku_num'] as bigint), cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint))      sku_num,\n" +
                        "       data['img_url']                                                                                                                     img_url,\n" +
                        "       data['sku_name']                                                                                                                    sku_name,\n" +
                        "       data['is_checked']                                                                                                                  is_checked,\n" +
                        "       data['create_time']                                                                                                                 create_time,\n" +
                        "       data['operate_time']                                                                                                                operate_time,\n" +
                        "       data['is_ordered']                                                                                                                  is_ordered,\n" +
                        "       data['order_time']                                                                                                                  order_time,\n" +
                        "       ts\n" +
                        // "       proc_time\n" +
                        "from topic_db_source\n" +
                        "where `database` = 'gmall'\n" +
                        "  and `table` = 'cart_info'\n" +
                        "  and (`type` = 'insert' or " +
                        "(`type` = 'update' and `old`['sku_num'] is not null and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))"
        );
    }
    
    public void createTopicDwdTradeCartAddToKafka(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE topic_dwd_trade_cart_add_sink\n" +
                "(\n" +
                "    `id`               STRING,\n" +
                "    `user_id`          STRING,\n" +
                "    `sku_id`           STRING,\n" +
                "    `cart_price`       STRING,\n" +
                "    `sku_num`          BIGINT,\n" +
                "    `img_url`          STRING,\n" +
                "    `sku_name`         STRING,\n" +
                "    `is_checked`       STRING,\n" +
                "    `create_time`      STRING,\n" +
                "    `operate_time`     STRING,\n" +
                "    `is_ordered`       STRING,\n" +
                "    `order_time`       STRING,\n" +
                "    `ts`               BIGINT\n" +
                ")" + FlinkSQLUtils.withSQLToKafka(Constants.TOPIC_DWD_TRADE_CART_ADD));
    }
}
