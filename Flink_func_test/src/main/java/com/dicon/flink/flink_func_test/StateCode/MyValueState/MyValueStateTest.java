package com.dicon.flink.flink_func_test.StateCode.MyValueState;

import com.dicon.flink.flink_func_test.StateCode.ClickSource;
import com.dicon.flink.flink_func_test.StateCode.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: dyc
 * @Create: 2023/7/11 14:04
 */

public class MyValueStateTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getLoginTime().getTime();
                            }
                        }));
        eventSingleOutputStreamOperator.print();

        eventSingleOutputStreamOperator.keyBy(event -> event.userName)
                .process(new PeriodPvResult())
                .print();

        env.execute();

    }

    public static class PeriodPvResult extends KeyedProcessFunction<String,Event,String>{

        //定义状态，保存当前pv统计值
        ValueState<Long> countState;

        //判断是否有定时器
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState",Long.class));

            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState",Long.class));

        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

            //每来一条数据更新状态
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);

            //注册定时器
            if (timerState.value() == null){
                ctx.timerService().registerEventTimeTimer(value.getLoginTime().getTime() + 10*1000L);
                timerState.update(value.getLoginTime().getTime() + 10*1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            out.collect(ctx.getCurrentKey() + " pv: " + countState.value());

            timerState.clear();
            ctx.timerService().registerEventTimeTimer(timestamp + 10*1000L);
            timerState.update(timestamp + 10*1000L);
        }
    }
}
