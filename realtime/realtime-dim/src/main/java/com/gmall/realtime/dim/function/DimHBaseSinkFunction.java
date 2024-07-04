package com.gmall.realtime.dim.function;

import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.bean.TableProcessDim;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.HBaseUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

public class DimHBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection connection;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtils.createConnection();
    }
    
    @Override
    public void close() throws Exception {
        HBaseUtils.closeConnection(connection);
    }
    
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObject = value.f0;
        TableProcessDim tableProcessDim = value.f1;
        
        String type = jsonObject.getString("type");
        JSONObject data = jsonObject.getJSONObject("data");
        String table = tableProcessDim.getSinkTable();
        String sinkRowKey = tableProcessDim.getSinkRowKey();
        String rowKey = data.getString(sinkRowKey);
        String sinkFamily = tableProcessDim.getSinkFamily();
        if ("delete".equals(type)) {
            HBaseUtils.delete(connection, Constants.HBASE_NAMESPACE, table, rowKey);
        } else {
            HBaseUtils.put(connection, Constants.HBASE_NAMESPACE, table, rowKey, sinkFamily, data);
        }
    }
}
