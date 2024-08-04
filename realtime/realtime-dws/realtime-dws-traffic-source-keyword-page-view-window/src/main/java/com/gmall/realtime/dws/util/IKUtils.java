package com.gmall.realtime.dws.util;

import lombok.SneakyThrows;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IKUtils {
    @SneakyThrows
    public static List<String> ikSplit(String keywords) {
        ArrayList<String> list = new ArrayList<>();
        StringReader stringReader = new StringReader(keywords);
        
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            list.add(next.getLexemeText());
            next = ikSegmenter.next();
        }
        return list;
    }
    
    public static void main(String[] args) throws IOException {
        String txt = "苹果Apple iPhone12mini 支持移动电信联通5G手机库存机 iPhone12mini【绿色】5.4英寸 128G【快充套装 店保两年】";
        StringReader stringReader = new StringReader(txt);
        
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            System.out.println(next.getLexemeText());
            next = ikSegmenter.next();
        }
    }
}
