package com.dicon.flink.flink_window_test.TumblingWindows;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author :dyc
 * @date: 2023/04/21
 */
public class tumblingWindow {
    public static void main(String[] args) throws Exception {

        //TODO 配置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 配置source
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.56.104",9999);

        //TODO transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                        //拆分word
                        String[] words = s.split(" ");

                        //遍历words 返回"[word,1]"
                        for (int i = 0; i < words.length; i++) {

                            collector.collect(new Tuple2<>(words[i], 1));
                        }
                    }
                }).keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        sum.print();

        //TODO sink
        env.execute("tumblingWindowTest");

    }


}
