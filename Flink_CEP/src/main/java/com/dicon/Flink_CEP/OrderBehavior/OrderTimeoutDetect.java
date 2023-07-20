package com.dicon.Flink_CEP.OrderBehavior;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 用户超时未支付告警
 * @Author: dyc
 * @Create: 2023/7/20 0:45
 */

public class OrderTimeoutDetect {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderEventSingleOutputStreamOperator = env.fromElements(
                new OrderEvent("user_1", "order_1", "create", 1000L),
                new OrderEvent("user_2", "order_2", "create", 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                        return orderEvent.timestamp;
                    }
                }));

        //TODO 定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));

        //TODO 将模式应用到流上
        PatternStream<OrderEvent> orderEventPatternStream = CEP.pattern(orderEventSingleOutputStreamOperator.keyBy(x -> x.userId), pattern);

        //TODO 定义侧输出流
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        //TODO 处理
        SingleOutputStreamOperator<String> process = orderEventPatternStream.process(new OrderPayMatch());
        process.print("payed: ");
        process.getSideOutput(timeoutTag)
                .print("timeout: ");

        env.execute();

    }

    public static class OrderPayMatch extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent>{

        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> collector) throws Exception {

            //获取当前的支付时间
            OrderEvent orderEvent = map.get("pay").get(0);
            collector.collect("用户：" + orderEvent.userId + " 订单" + orderEvent.orderId);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context context) throws Exception {

            OrderEvent orderEvent = map.get("create").get(0);

            OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

            context.output(timeoutTag,"用户：" + orderEvent.userId + " 订单" + orderEvent.orderId + " 超时未支付");


        }
    }
}
