package com.gmall.realtime.common.util;


import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.constant.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Flink Sink工具类
 */
public class FlinkSinkUtils {
    public static KafkaSink<String> getKafkaSink(String topicName) {
        Properties properties = new Properties();
        properties.put("transaction.timeout.ms", 1000 * 60 * 15);
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                                               .setBootstrapServers(Constants.KAFKA_BOOTSTRAP_SERVERS)
                                               .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                                                       .setTopic(topicName)
                                                       .setValueSerializationSchema(new SimpleStringSchema())
                                                       .build())
                                               .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                               .setTransactionalIdPrefix("gmall" + "-" + topicName + "-" + System.currentTimeMillis())
                                               .setKafkaProducerConfig(properties)
                                               .build();
        return kafkaSink;
    }
    
    public static KafkaSink<JSONObject> getKafkaSink() {
        Properties properties = new Properties();
        properties.put("transaction.timeout.ms", 1000 * 60 * 15);
        KafkaSink<JSONObject> kafkaSink = KafkaSink.<JSONObject>builder()
                                                   .setBootstrapServers(Constants.KAFKA_BOOTSTRAP_SERVERS)
                                                   .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                                                       @Override
                                                       public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaSinkContext context, Long timestamp) {
                                                           String topic = element.getString("sink_table");
                                                           return new ProducerRecord<>(topic, element.toJSONString().getBytes());
                                                       }
                                                   })
                                                   .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                                   .setTransactionalIdPrefix("gmall" + "-" + "base_db" + "-" + System.currentTimeMillis())
                                                   .setKafkaProducerConfig(properties)
                                                   .build();
        return kafkaSink;
    }
}
