package com.gmall.realtime.dws;

import com.gmall.realtime.common.base.BaseSQLApp;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.FlinkSQLUtils;
import com.gmall.realtime.dws.function.KeywordSpiltFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域搜索关键词粒度页面浏览各窗口汇总表
 */
public class DwsTrafficSourceKeywordPageViewWindowApp extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficSourceKeywordPageViewWindowApp().start(10021, 4, "dws-traffic-source-keyword-page-view-window-app");
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table page_info_source\n" +
                "(\n" +
                "    common map<STRING, STRING>,\n" +
                "    page   map<STRING, STRING>,\n" +
                "    ts     bigint,\n" +
                "    event_time as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '10' second\n" +
                ")" + FlinkSQLUtils.withSQLFromKafka(Constants.TOPIC_DWD_TRAFFIC_PAGE, Constants.TOPIC_DWD_TRAFFIC_PAGE));
        Table pageInfo = tEnv.sqlQuery(
                "select " +
                        "       page['item'] keywords,\n" +
                        "       ts,\n" +
                        "       event_time\n" +
                        "from page_info_source\n" +
                        "where page['item'] is not null\n" +
                        "  and page['item_type'] = 'keyword'\n" +
                        "  and page['last_page_id'] = 'search'\n" +
                        "  and (cast(ts as bigint)  > 1722441600000 and cast(ts as bigint) < 1722787200000)"
        );
        tEnv.createTemporaryView("page_info", pageInfo);
        
        // 注册函数
        tEnv.createTemporarySystemFunction("KeywordSpiltFunction", KeywordSpiltFunction.class);
        
        Table keywordInfo = tEnv.sqlQuery(
                "SELECT keywords, keyword, event_time " +
                        "FROM page_info, LATERAL TABLE(KeywordSpiltFunction(keywords))"
        );
        tEnv.createTemporaryView("keyword_info", keywordInfo);
        
        Table windowAggTable = tEnv.sqlQuery(
                "SELECT " +
                        "       cast(TUMBLE_START(event_time, INTERVAL '10' second) as STRING) AS stt,\n" +
                        "       cast(TUMBLE_END(event_time, INTERVAL '10' second) as STRING)   AS edt,\n" +
                        "       cast(current_date as STRING) as cur_date,\n" +
                        "       keyword,\n" +
                        "       count(*)                                       as keyword_count\n" +
                        "from keyword_info\n" +
                        "GROUP BY TUMBLE(event_time, INTERVAL '10' second), keyword"
        );
        
        tEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window_sink\n" +
                "(\n" +
                "    stt           STRING,\n" +
                "    edt           STRING,\n" +
                "    cur_date      STRING,\n" +
                "    keyword       STRING,\n" +
                "    keyword_count BIGINT\n" +
                ")" + FlinkSQLUtils.withSQLToDoris(Constants.DORIS_TABLE_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
        
        windowAggTable.insertInto("dws_traffic_source_keyword_page_view_window_sink").execute();
    }
}
