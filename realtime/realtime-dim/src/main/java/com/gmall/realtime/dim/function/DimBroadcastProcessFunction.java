package com.gmall.realtime.dim.function;

import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.bean.TableProcessDim;
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

public class DimBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    
    private final MapStateDescriptor<String, TableProcessDim> processDimMapStateDescriptor;
    private final HashMap<String, TableProcessDim> map = new HashMap<>();
    
    public DimBroadcastProcessFunction(MapStateDescriptor<String, TableProcessDim> processDimMapStateDescriptor) {
        this.processDimMapStateDescriptor = processDimMapStateDescriptor;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 预加载初始的维度表
        Connection mySqlConnection = JdbcUtils.getMySqlConnection();
        List<TableProcessDim> list = JdbcUtils.queryList(mySqlConnection, "select * from gmall_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : list) {
            tableProcessDim.setOp("r");
            map.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtils.closeConnection(mySqlConnection);
    }
    
    // 处理广播流数据
    @Override
    public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(processDimMapStateDescriptor);
        String op = value.getOp();
        if ("d".equals(op)) {
            broadcastState.remove(value.getSourceTable());
            map.remove(value.getSourceTable());
        } else {
            broadcastState.put(value.getSourceTable(), value);
        }
    }
    
    // 处理主流数据
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(processDimMapStateDescriptor);
        String table = value.getString("table");
        TableProcessDim tableProcessDim = broadcastState.get(table);
        if (tableProcessDim == null) {
            tableProcessDim = map.get(table);
        }
        if (tableProcessDim != null) {
            out.collect(Tuple2.of(value, tableProcessDim));
        }
    }
}
