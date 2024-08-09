package com.gmall.realtime.dws;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.TradeSkuOrder;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.DateFormatUtils;
import com.gmall.realtime.common.util.FlinkSinkUtils;
import com.gmall.realtime.common.util.HBaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * 交易域SKU粒度下单各窗口汇总表
 */
public class DwsTradeSkuOrderWindowApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindowApp().start(10029, 4, "dws-trade-sku-order-window-app", Constants.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        // 过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                boolean isJsonObject = JSON.isValidObject(value);
                if (isJsonObject) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String id = jsonObject.getString("id");
                    String skuId = jsonObject.getString("sku_id");
                    Long ts = jsonObject.getLong("ts");
                    if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(skuId) && ts != null) {
                        jsonObject.put("ts", ts * 1000);
                        out.collect(jsonObject);
                    }
                }
            }
        });
        
        // 添加水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjectStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                 .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                     @Override
                                     public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                         return element.getLong("ts");
                                     }
                                 })
        );
        
        // 按照id进行分区
        KeyedStream<JSONObject, String> keyedByIdStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });
        
        // 修正度量值，转换数据结构
        SingleOutputStreamOperator<TradeSkuOrder> beanStream = keyedByIdStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrder>() {
            MapState<String, BigDecimal> lastAmountState;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmountStateDescriptor = new MapStateDescriptor<>("last-amount", String.class, BigDecimal.class);
                lastAmountStateDescriptor.enableTimeToLive(
                        StateTtlConfig.newBuilder(Time.seconds(30L))
                                      .updateTtlOnCreateAndWrite()
                                      .build()
                );
                lastAmountState = getRuntimeContext().getMapState(lastAmountStateDescriptor);
            }
            
            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradeSkuOrder>.Context ctx, Collector<TradeSkuOrder> out) throws Exception {
                /*
                 * +I   1001         null        null
                 * -I   1001         null        null
                 * +I   1001         1           null
                 * -I   1001         1           null
                 * +I   1001         1           2
                 *  */
                BigDecimal originalAmount = lastAmountState.get("originalAmount");
                BigDecimal activityReduceAmount = lastAmountState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = lastAmountState.get("couponReduceAmount");
                BigDecimal orderAmount = lastAmountState.get("orderAmount");
                
                originalAmount = originalAmount == null ? BigDecimal.valueOf(0) : originalAmount;
                activityReduceAmount = activityReduceAmount == null ? BigDecimal.valueOf(0) : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? BigDecimal.valueOf(0) : couponReduceAmount;
                orderAmount = orderAmount == null ? BigDecimal.valueOf(0) : orderAmount;
                
                lastAmountState.put("originalAmount", value.getBigDecimal("order_price").multiply(value.getBigDecimal("sku_num")));
                lastAmountState.put("activityReduceAmount", value.getBigDecimal("split_activity_amount"));
                lastAmountState.put("couponReduceAmount", value.getBigDecimal("split_coupon_amount"));
                lastAmountState.put("orderAmount", value.getBigDecimal("split_total_amount"));
                
                out.collect(TradeSkuOrder.builder()
                                         .orderDetailId(value.getString("id"))
                                         .skuId(value.getString("sku_id"))
                                         .originalAmount(value.getBigDecimal("order_price").multiply(value.getBigDecimal("sku_num")).subtract(originalAmount))
                                         .activityReduceAmount(value.getBigDecimal("split_activity_amount").subtract(activityReduceAmount))
                                         .couponReduceAmount(value.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount))
                                         .orderAmount(value.getBigDecimal("split_total_amount").subtract(orderAmount))
                                         .ts(value.getLong("ts"))
                                         .build());
            }
        });
        
        // 分区
        KeyedStream<TradeSkuOrder, String> keyedBySkuIdStream = beanStream.keyBy(new KeySelector<TradeSkuOrder, String>() {
            @Override
            public String getKey(TradeSkuOrder value) throws Exception {
                return value.getSkuId();
            }
        });
        
        // 开窗
        WindowedStream<TradeSkuOrder, String, TimeWindow> windowedStream =
                keyedBySkuIdStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        
        // 聚合
        SingleOutputStreamOperator<TradeSkuOrder> reducedStream = windowedStream.reduce(new ReduceFunction<TradeSkuOrder>() {
            @Override
            public TradeSkuOrder reduce(TradeSkuOrder value1, TradeSkuOrder value2) throws Exception {
                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<TradeSkuOrder, TradeSkuOrder, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TradeSkuOrder, TradeSkuOrder, String, TimeWindow>.Context context, Iterable<TradeSkuOrder> elements, Collector<TradeSkuOrder> out) throws Exception {
                TimeWindow window = context.window();
                
                String stt = DateFormatUtils.tsToDateTimeString(window.getStart());
                String edt = DateFormatUtils.tsToDateTimeString(window.getEnd());
                String curDate = DateFormatUtils.tsToDateString(System.currentTimeMillis());
                for (TradeSkuOrder tradeSkuOrder : elements) {
                    tradeSkuOrder.setStt(stt);
                    tradeSkuOrder.setEdt(edt);
                    tradeSkuOrder.setCurDate(curDate);
                    out.collect(tradeSkuOrder);
                }
            }
        });
        
        // 关联维度信息
        SingleOutputStreamOperator<TradeSkuOrder> fullDimStream = reducedStream.map(new RichMapFunction<TradeSkuOrder, TradeSkuOrder>() {
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
            public TradeSkuOrder map(TradeSkuOrder value) throws Exception {
                JSONObject dimSkuInfo = HBaseUtils.get(connection, Constants.HBASE_NAMESPACE, Constants.HBASE_TABLE_DIM_SKU_INFO, value.getSkuId(), Constants.HBASE_COLUMNFAMILY);
                value.setTrademarkId(dimSkuInfo.getString("tm_id"));
                value.setCategory3Id(dimSkuInfo.getString("category3_id"));
                value.setSkuName(dimSkuInfo.getString("sku_name"));
                value.setSpuId(dimSkuInfo.getString("spu_id"));
                
                JSONObject dimSpuInfo = HBaseUtils.get(connection, Constants.HBASE_NAMESPACE, Constants.HBASE_TABLE_DIM_SPU_INFO, value.getSpuId(), Constants.HBASE_COLUMNFAMILY);
                value.setSpuName(dimSpuInfo.getString("spu_name"));
                
                JSONObject dimBaseCategory3 = HBaseUtils.get(connection, Constants.HBASE_NAMESPACE, Constants.HBASE_TABLE_DIM_BASE_CATEGORY3, value.getCategory3Id(), Constants.HBASE_COLUMNFAMILY);
                value.setCategory2Id(dimBaseCategory3.getString("category2_id"));
                value.setCategory3Name(dimBaseCategory3.getString("name"));
                
                JSONObject dimBaseCategory2 = HBaseUtils.get(connection, Constants.HBASE_NAMESPACE, Constants.HBASE_TABLE_DIM_BASE_CATEGORY2, value.getCategory2Id(), Constants.HBASE_COLUMNFAMILY);
                value.setCategory1Id(dimBaseCategory2.getString("category1_id"));
                value.setCategory2Name(dimBaseCategory2.getString("name"));
                
                JSONObject dimBaseCategory1 = HBaseUtils.get(connection, Constants.HBASE_NAMESPACE, Constants.HBASE_TABLE_DIM_BASE_CATEGORY1, value.getCategory1Id(), Constants.HBASE_COLUMNFAMILY);
                value.setCategory1Name(dimBaseCategory1.getString("name"));
                
                JSONObject dimBaseTrademark = HBaseUtils.get(connection, Constants.HBASE_NAMESPACE, Constants.HBASE_TABLE_DIM_BASE_TRADEMARK, value.getTrademarkId(), Constants.HBASE_COLUMNFAMILY);
                value.setTrademarkName(dimBaseTrademark.getString("tm_name"));
                return value;
            }
        });
        
        SingleOutputStreamOperator<String> mapStream = fullDimStream.map(JSON::toJSONString);
        mapStream.sinkTo(FlinkSinkUtils.getDorisSink(Constants.DORIS_TABLE_DWS_TRADE_SKU_ORDER_WINDOW));
    }
}
