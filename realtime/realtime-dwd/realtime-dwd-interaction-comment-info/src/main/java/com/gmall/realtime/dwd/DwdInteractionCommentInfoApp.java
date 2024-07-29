package com.gmall.realtime.dwd;

import com.gmall.realtime.common.base.BaseSQLApp;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.FlinkSQLUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfoApp extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdInteractionCommentInfoApp().start(10012, 4, "dwd-interaction-comment-info-app");
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        createTopicDbFromKafka(tEnv);
        
        createBaseDicFromHBase(tEnv);
        
        Table commentInfo = selectCommentInfo(tEnv);
        tEnv.createTemporaryView("comment_info", commentInfo);
        
        Table joinTable = tEnv.sqlQuery("select " +
                "       id,\n" +
                "       user_id,\n" +
                "       nick_name,\n" +
                "       sku_id,\n" +
                "       spu_id,\n" +
                "       order_id,\n" +
                "       appraise appraise_code,\n" +
                "       info.dic_name appraise_name,\n" +
                "       comment_txt,\n" +
                "       create_time,\n" +
                "       operate_time\n" +
                "from comment_info c\n" +
                "         join base_dic for system_time as of c.proc_time as b\n" +
                "on c.appraise = b.rowkey");
        
        createTopicDwdInteractionCommentInfoToKafka(tEnv);
        joinTable.insertInto(Constants.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
    }
    
    private Table selectCommentInfo(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery(
                "select " +
                        "       data['id']           id,\n" +
                        "       data['user_id']      user_id,\n" +
                        "       data['nick_name']    nick_name,\n" +
                        "       data['head_img']     head_img,\n" +
                        "       data['sku_id']       sku_id,\n" +
                        "       data['spu_id']       spu_id,\n" +
                        "       data['order_id']     order_id,\n" +
                        "       data['appraise']     appraise,\n" +
                        "       data['comment_txt']  comment_txt,\n" +
                        "       data['create_time']  create_time,\n" +
                        "       data['operate_time'] operate_time,\n" +
                        "       proc_time\n" +
                        "from topic_db\n" +
                        "where `database` = 'gmall'\n" +
                        "  and `table` = 'comment_info'\n" +
                        "  and `type` = 'insert'"
        );
    }
    
    private void createTopicDwdInteractionCommentInfoToKafka(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE " + Constants.TOPIC_DWD_INTERACTION_COMMENT_INFO + "\n" +
                "(\n" +
                "    `id`               STRING,\n" +
                "    `user_id`          STRING,\n" +
                "    `nick_name`        STRING,\n" +
                "    `sku_id`           STRING,\n" +
                "    `spu_id`           STRING,\n" +
                "    `order_id`         STRING,\n" +
                "    `appraise_code`    STRING,\n" +
                "    `appraise_name`    STRING,\n" +
                "    `comment_txt`      STRING,\n" +
                "    `create_time`      STRING,\n" +
                "    `operate_time`     STRING\n" +
                ")" + FlinkSQLUtils.withSQLToKafka(Constants.TOPIC_DWD_INTERACTION_COMMENT_INFO));
    }
}
