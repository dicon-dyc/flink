package com.dicon.flink.flink_func_test.Flink_Connect.duizhang;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: dyc
 * @Create: 2023/7/5 13:25
 */

public class duizhang {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String,String,Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                return stringStringLongTuple3.f2;
            }
        }));

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartStream = env.fromElements(
                Tuple4.of("order-1", "third-part", "success", 3000L),
                Tuple4.of("order-2", "third-part", "success", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple4<String, String, String, Long> stringStringStringLongTuple4, long l) {
                return stringStringStringLongTuple4.f3;
            }
        }));

        appStream.connect(thirdPartStream)
                .keyBy(data -> data.f0,data -> data.f0)
                .process(new MyOrderMatch())
                .print();

        env.execute();

    }

    public static class MyOrderMatch extends CoProcessFunction<Tuple3<String,String,Long>,Tuple4<String,String,String,Long>,String>{

        private ValueState<Tuple3<String,String,Long>> appEventState;

        private ValueState<Tuple4<String,String,String,Long>> thirdPartEventState;

        @Override
        public void open(Configuration parameters) throws Exception {

            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
            );

            thirdPartEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdpart-event",Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.LONG))

            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {

            if (thirdPartEventState.value() != null){
                out.collect("对账成功：" + value + " " + thirdPartEventState.value());
                thirdPartEventState.clear();
            }else {
                appEventState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f2 + 500L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {

            if (appEventState.value() != null){
                out.collect("对账成功：" + appEventState.value() + " " + value);
                appEventState.clear();
            }else {
                thirdPartEventState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f3 + 500L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (appEventState.value() != null){
                out.collect("对账失败：" + appEventState.value() +" " + "第三方支付平台信息未到");
            }

            if (thirdPartEventState.value() != null){
                out.collect("对账失败：" + thirdPartEventState.value() +" " + "app信息未到");
            }
        }
    }
}
