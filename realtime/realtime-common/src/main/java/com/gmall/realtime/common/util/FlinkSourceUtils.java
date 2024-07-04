package com.gmall.realtime.common.util;

import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.schema.MySimpleStringSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

/**
 * Flink Source工具类
 */
public class FlinkSourceUtils {
    public static KafkaSource<String> getKafkaSource(String groupId, String topicName) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                                                     .setBootstrapServers(Constants.KAFKA_BOOTSTRAP_SERVERS)
                                                     .setTopics(topicName)
                                                     .setStartingOffsets(OffsetsInitializer.earliest())
                                                     .setGroupId(groupId)
                                                     .setValueOnlyDeserializer(
                                                             // new SimpleStringSchema()
                                                             new MySimpleStringSchema()// 解决null值问题
                                                     )
                                                     .build();
        return kafkaSource;
        
    }
    
    public static MySqlSource<String> getMySqlSource(String databaseName, String tableName) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                                                     .hostname(Constants.MYSQL_HOST)
                                                     .port(Constants.MYSQL_PORT)
                                                     .username(Constants.MYSQL_USERNAME)
                                                     .password(Constants.MYSQL_PASSWORD)
                                                     .jdbcProperties(props)
                                                     .databaseList(databaseName)
                                                     .tableList(databaseName + "." + tableName)
                                                     .deserializer(new JsonDebeziumDeserializationSchema())
                                                     .startupOptions(StartupOptions.initial())
                                                     .build();
        return mySqlSource;
        
    }
}
