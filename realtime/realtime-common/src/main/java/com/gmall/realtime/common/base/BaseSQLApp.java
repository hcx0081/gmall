package com.gmall.realtime.common.base;

import com.gmall.realtime.common.util.FlinkSQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 项目基类
 */
public abstract class BaseSQLApp {
    public void start(int port, int parallelism, String ckps) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.100.100:8020/gmall/flink/ckps/" + ckps);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        handle(env, tEnv);
    }
    
    protected void createTopicDbFromKafka(StreamTableEnvironment tEnv) {
        tEnv.executeSql(FlinkSQLUtils.createTopicDbFromKafka());
    }
    
    protected void createBaseDicFromHBase(StreamTableEnvironment tEnv) {
        tEnv.executeSql(FlinkSQLUtils.createBaseDicFromHBase());
    }
    
    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);
}
