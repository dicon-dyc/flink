package com.dicon.flink.flink_window_test.SlidingWindows;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @Author:dyc
 * @Date:2023/04/21
 */

public class slidingWindow {

    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 添加source
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.56.104",9999);


        //TODO transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                        //拆分s
                        String[] words = s.split(" ");

                        //转换为[word,1]并返回
                        for (int i = 0; i < words.length; i++) {
                            collector.collect(new Tuple2<>(words[i], 1));
                        }
                    }
                }).keyBy(x -> x.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5)))
                .sum(1);

        //TODO sink
        sum.print();

        env.execute("slidingWindow");

    }

}
