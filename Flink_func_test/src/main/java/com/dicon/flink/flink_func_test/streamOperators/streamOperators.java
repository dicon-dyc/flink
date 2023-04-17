package com.dicon.flink.flink_func_test.streamOperators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author:dyc
 * Date:2023-04-16
 */
public class streamOperators {
    public static void main(String[] args) throws Exception {

        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 配置source
        /**
         * home:192.168.245.141
         * work:192.168.56.103
         */
        DataStreamSource streamSource = env.socketTextStream("192.168.56.103",9999);

        //TODO 计算

        /**
         * map:将dataStream中的每个元素转换为另一个元素
         */
        //DataStream<String> resultData = streamSource.map(x -> "map : " + x);

        /**
         * flatMap:获取一个数据元并生成0，1，n个数据元。
         */
        DataStream<String> resultData = streamSource.flatMap(new FlatMapFunction<String,String>() {

            @Override
            public void flatMap(String input, Collector collector) throws Exception {

                String[] words = input.split(" ");
                for (int i = 0; i < words.length; i++) {
                    collector.collect(words[i]);
                }
            }
        });

        //TODO 输入
        resultData.print();
        env.execute();


    }
}
