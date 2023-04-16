package com.dicon.flink.flink_func_test.streamOperators;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author:dyc
 * Date:2023-04-16
 */
public class streamOperators {
    public static void main(String[] args) throws Exception {

        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 配置source
        DataStreamSource streamSource = env.socketTextStream("192.168.245.141",9999);

        //TODO 计算

        /**
         * map:将dataStream中的每个元素转换为另一个元素
         */
        //DataStream<String> resultData = streamSource.map(x -> "map : " + x);


        //TODO 输入
        resultData.print();
        env.execute();


    }
}
