package com.dicon.flink.flink_func_test.Flink_Side_Output;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author:DYC
 * @Date:2023/04/29
 */
public class Flink_filter {

    public static void main(String[] args) throws Exception {

        //TODO 配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 配置、读取source
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("192.168.245.141", 9999);

        //TODO transform

        //将流转换为UserPOJO类型
        SingleOutputStreamOperator<UserPOJO> mapDataStream = stringDataStreamSource.map(x -> new UserPOJO(Arrays.stream(x.split(",")).toArray()[0].toString(), Arrays.stream(x.split(",")).toArray()[1].toString()));

        //分出名字为dyc的流
        SingleOutputStreamOperator<UserPOJO> filterDataStream = mapDataStream.filter(new FilterFunction<UserPOJO>() {
            @Override
            public boolean filter(UserPOJO userPOJO) throws Exception {

                if ("dyc".equals(userPOJO.getUserName())) {
                    return true;
                }
                return false;
            }
        });

        //TODO 配置sink并输出
        filterDataStream.print();

        env.execute("filter_test");


    }
}
