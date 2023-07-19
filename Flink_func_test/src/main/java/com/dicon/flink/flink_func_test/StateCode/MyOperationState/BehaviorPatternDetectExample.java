package com.dicon.flink.flink_func_test.StateCode.MyOperationState;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.*;

/**
 * @Author: dyc
 * @Create: 2023/7/11 20:19
 */
public class BehaviorPatternDetectExample {

    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //用户的行为数据流
        DataStreamSource<Action> actionDataStreamSource = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "order")
        );

        //行为模式流
        DataStreamSource<Pattern> patternDataStreamSource = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );

        //定义广播状态描述器
        MapStateDescriptor<Void, Pattern> patternDescriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternDataStreamSource.broadcast(patternDescriptor);

        SingleOutputStreamOperator<Tuple2<String,Pattern>> matchStream = actionDataStreamSource.keyBy(x -> x.userId)
                .connect(broadcastStream)
                .process(new PatternDetector());

        matchStream.print();
        env.execute();


    }

    public static class PatternDetector extends KeyedBroadcastProcessFunction<String,Action,Pattern, Tuple2<String,Pattern>>{

        //定义一个keyedstate，保存上一次用户的行为
        ValueState<String> preActionState;

        @Override
        public void open(Configuration parameters) throws Exception {

            preActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("preActionstate", String.class));
        }

        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {

            //从广播状态中获取匹配模式
            ReadOnlyBroadcastState<Void, Pattern> pattern = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));

            Pattern pattern1 = pattern.get(null);

            //获取用户上次行为
            String value1 = preActionState.value();

            //判断是否匹配
            if (pattern1 != null){
                if (pattern1.action1.equals(value1) && pattern1.action2.equals(value.action)){
                    out.collect(new Tuple2<>(ctx.getCurrentKey(),pattern1));
                }
            }
            //状态更新
            preActionState.update(value.action);

        }

        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {

            //从上下文中获取广播状态，并用当前数据更新状态
            BroadcastState<Void, Pattern> pattern = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));

            pattern.put(null,value);

        }
    }


    //定义用户行为和模式的pojo类
    public static class Action{

        public String userId;
        public String action;

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        public Action() {
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    //模式流
    public static class Pattern{

        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        public String getAction1() {
            return action1;
        }

        public void setAction1(String action1) {
            this.action1 = action1;
        }

        public String getAction2() {
            return action2;
        }

        public void setAction2(String action2) {
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
