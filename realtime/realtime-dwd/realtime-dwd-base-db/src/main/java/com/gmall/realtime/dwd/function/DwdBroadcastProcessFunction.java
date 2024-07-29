package com.gmall.realtime.dwd.function;

import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.bean.TableProcessDwd;
import com.gmall.realtime.common.util.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

public class DwdBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {
    
    private final MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    private final HashMap<String, TableProcessDwd> map = new HashMap<>();
    
    public DwdBroadcastProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 预加载初始的维度表
        Connection mySqlConnection = JdbcUtils.getMySqlConnection();
        List<TableProcessDwd> list = JdbcUtils.queryList(mySqlConnection, "select * from gmall_config.table_process_dwd", TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : list) {
            tableProcessDwd.setOp("r");
            String key = tableProcessDwd.getSourceTable() + "." + tableProcessDwd.getSourceType();
            map.put(key, tableProcessDwd);
        }
        JdbcUtils.closeConnection(mySqlConnection);
    }
    
    @Override
    public void processBroadcastElement(TableProcessDwd value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String op = value.getOp();
        String key = value.getSourceTable() + "." + value.getSourceType();
        if ("d".equals(op)) {
            broadcastState.remove(key);
            map.remove(key);
        } else {
            broadcastState.put(key, value);
        }
    }
    
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String table = value.getString("table");
        String type = value.getString("type");
        String key = table + "." + type;
        TableProcessDwd tableProcessDwd = broadcastState.get(key);
        if (tableProcessDwd == null) {
            tableProcessDwd = map.get(key);
        }
        if (tableProcessDwd != null) {
            out.collect(Tuple2.of(value, tableProcessDwd));
        }
    }
}
