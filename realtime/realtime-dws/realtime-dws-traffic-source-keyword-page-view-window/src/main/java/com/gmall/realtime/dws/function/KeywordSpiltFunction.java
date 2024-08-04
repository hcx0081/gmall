package com.gmall.realtime.dws.function;

import com.gmall.realtime.dws.util.IKUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KeywordSpiltFunction extends TableFunction<Row> {
    public void eval(String keywords) {
        List<String> list = IKUtils.ikSplit(keywords);
        for (String word : list) {
            collect(Row.of(word));
        }
    }
}