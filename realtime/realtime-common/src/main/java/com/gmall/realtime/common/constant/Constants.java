package com.gmall.realtime.common.constant;

/**
 * 常量类
 */
public interface Constants {
    String KAFKA_BOOTSTRAP_SERVERS = "192.168.100.100:9092";
    
    String TOPIC_DB = "topic_db";
    String TOPIC_LOG = "topic_log";
    
    String MYSQL_HOST = "192.168.100.100";
    int MYSQL_PORT = 3306;
    String MYSQL_USERNAME = "root";
    String MYSQL_PASSWORD = "200081";
    
    String CONFIG_DATABASE = "gmall_config";
    String CONFIG_TABLE = "table_process_dim";
    
    String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    String MYSQL_URL = "jdbc:mysql://192.168.100.100:3306?useSSL=false";
    
    String HBASE_ZOOKEEPER_QUORUM = "192.168.100.100";
    String HBASE_NAMESPACE = "gmall";
    
    /* 主题 */
    String TOPIC_DWD_TRAFFIC_START = "topic_dwd_traffic_start";
    String TOPIC_DWD_TRAFFIC_ERR = "topic_dwd_traffic_err";
    String TOPIC_DWD_TRAFFIC_PAGE = "topic_dwd_traffic_page";
    String TOPIC_DWD_TRAFFIC_ACTION = "topic_dwd_traffic_action";
    String TOPIC_DWD_TRAFFIC_DISPLAY = "topic_dwd_traffic_display";
    
    String TOPIC_DWD_INTERACTION_COMMENT_INFO = "topic_dwd_interaction_comment_info";
    
    String TOPIC_DWD_TRADE_CART_ADD = "topic_dwd_trade_cart_add";
    
    String TOPIC_DWD_TRADE_ORDER_DETAIL = "topic_dwd_trade_order_detail";
    
    String TOPIC_DWD_TRADE_ORDER_CANCEL = "topic_dwd_trade_order_cancel";
    
    String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "topic_dwd_trade_order_payment_success";
    
    String TOPIC_DWD_TRADE_ORDER_REFUND = "topic_dwd_trade_order_refund";
    
    String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "topic_dwd_trade_refund_payment_success";
    
    String TOPIC_DWD_USER_REGISTER = "topic_dwd_user_register";
}
