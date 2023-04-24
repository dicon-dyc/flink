package com.dicon.flink.flink_func_test.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author:dyc
 * Date:2023/04/16
 */

public class wordCount {

    private static KeySelector keySelector;

    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 配置source，读取数据
        DataStreamSource streamSource = env.socketTextStream("192.168.56.104",9999);

        //TODO 计算
        DataStream<Tuple2<String,Integer>> resultStream = streamSource.filter(x -> !x.equals("flink")).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String words, Collector collector) throws Exception {
                for (String word : words.split(" ")) {
                    Tuple2<String, Integer> input = new Tuple2<>(word, 1);
                    collector.collect(input);
                }
            }
        }).keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0)
                .sum(1);

        //TODO 输出
        resultStream.print();
        env.execute();


    }
}
