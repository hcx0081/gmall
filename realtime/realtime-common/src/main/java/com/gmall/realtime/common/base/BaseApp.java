package com.gmall.realtime.common.base;

import com.gmall.realtime.common.util.FlinkSourceUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 项目基类
 */
public abstract class BaseApp {
    public void start(int port, int parallelism, String ckpsAndGroupId, String topicName) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        
        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000);
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.100.100:8020/gmall/flink/ckps/" + ckpsAndGroupId);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // env.getCheckpointConfig().setCheckpointTimeout(10000);
        // env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        DataStreamSource<String> kafkaSource = env.fromSource(
                FlinkSourceUtils.getKafkaSource(ckpsAndGroupId, topicName),
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );
        
        handle(env, kafkaSource);
        
        env.execute();
    }
    
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> source);
}
