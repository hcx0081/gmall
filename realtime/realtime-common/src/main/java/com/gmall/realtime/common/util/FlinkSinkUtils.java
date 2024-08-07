package com.gmall.realtime.common.util;


import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.constant.Constants;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
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
    public static KafkaSink<String> getKafkaSink(String topic) {
        Properties properties = new Properties();
        properties.put("transaction.timeout.ms", 1000 * 60 * 15);
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                                               .setBootstrapServers(Constants.KAFKA_BOOTSTRAP_SERVERS)
                                               .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                                                       .setTopic(topic)
                                                       .setValueSerializationSchema(new SimpleStringSchema())
                                                       .build())
                                               .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                               .setTransactionalIdPrefix("gmall" + "-" + topic + "-" + System.currentTimeMillis())
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
                                                           String topic = "topic_" + element.getString("sink_table");
                                                           return new ProducerRecord<>(topic, element.toJSONString().getBytes());
                                                       }
                                                   })
                                                   .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                                   .setTransactionalIdPrefix("gmall" + "-" + "base_db" + "-" + System.currentTimeMillis())
                                                   .setKafkaProducerConfig(properties)
                                                   .build();
        return kafkaSink;
    }
    
    public static DorisSink<String> getDorisSink(String table) {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        DorisSink<String> dorisSink =
                DorisSink.<String>builder()
                         .setDorisReadOptions(DorisReadOptions.builder().build())
                         .setDorisExecutionOptions(DorisExecutionOptions.builder()
                                                                        .setLabelPrefix("label-doris-" + System.currentTimeMillis())
                                                                        .setDeletable(false)
                                                                        .setStreamLoadProp(properties).build())
                         .setSerializer(new SimpleStringSerializer())
                         .setDorisOptions(DorisOptions.builder()
                                                      .setFenodes(Constants.DORIS_FE_NODES)
                                                      .setTableIdentifier(Constants.DORIS_DATABASE + "." + table)
                                                      .setUsername(Constants.DORIS_USERNAME)
                                                      .setPassword(Constants.DORIS_PASSWORD)
                                                      .build())
                         .build();
        return dorisSink;
    }
}
