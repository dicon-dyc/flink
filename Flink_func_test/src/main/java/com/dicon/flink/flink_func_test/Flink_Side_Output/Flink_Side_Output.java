package com.dicon.flink.flink_func_test.Flink_Side_Output;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * @Author:DYC
 * @Date:2023/04/29
 *
 */
public class Flink_Side_Output {
    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setStateBackend("")

        //TODO 配置、读取source
        DataStreamSource<String> dataStreamSrouce = env.socketTextStream("192.168.245.141", 9999);

        //TODO transform

        //将输入流转化为实体类UserPOJO
        DataStream<UserPOJO> mapDataStream = dataStreamSrouce.map(x -> new UserPOJO(Arrays.stream(x.split(",")).toArray()[0].toString(),Arrays.stream(x.split(",")).toArray()[1].toString()));

        //创建分流表
        OutputTag<UserPOJO> dycTag = new OutputTag<UserPOJO>("dyc"){};
        OutputTag<UserPOJO> othersTag = new OutputTag<UserPOJO>("others"){};

        //分类处理
        SingleOutputStreamOperator<UserPOJO> processDataStream =  processDataStream = mapDataStream.process(new ProcessFunction<UserPOJO, UserPOJO>() {
            @Override
            public void processElement(UserPOJO value, Context ctx, Collector<UserPOJO> out) throws Exception {

                if ("dyc".equals(value.getUserName())){
                    ctx.output(dycTag,value);
                }else {
                    ctx.output(othersTag,value);
                }
            }
        });

        processDataStream.getSideOutput(dycTag).print();

        env.execute("sideoutputtest");



    }

}
