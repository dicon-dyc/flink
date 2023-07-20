package com.dicon.Flink_CEP.OrderBehavior;


import com.dicon.Flink_CEP.UserBehivor.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *
 * 状态机
 * @Author: dyc
 * @Create: 2023/7/20 13:36
 */

public class NFAExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KeyedStream<LoginEvent, String> orderEventStringKeyedStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "192.168.0.3", "fail", 5000L),
//                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.timestamp;
                    }
                })).keyBy(x -> x.userId);

        SingleOutputStreamOperator<String> warningStream = orderEventStringKeyedStream.flatMap(new StateMachineMaper());

        warningStream.print();

        env.execute();

    }

    public static class StateMachineMaper extends RichFlatMapFunction<LoginEvent, String> {

        //声名状态机当前的状态
        ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state",State.class));
        }

        @Override
        public void flatMap(LoginEvent loginEvent, Collector<String> collector) throws Exception {

            State state = currentState.value();

            if (state == null){
                state = State.Initial;
            }

            State nextState = state.transition(loginEvent.eventType);

            if (nextState == State.Matched){

                //检测到了匹配，输出报警信息。
                collector.collect(loginEvent.userId + "连续三次登陆失败");

            }else if (nextState == State.Terminal){

                currentState.update(State.Initial);
            }else {
                currentState.update(nextState);
            }

        }
    }

    //实现状态机
    public enum State{

        Terminal, //终止状态
        Matched, //匹配成功

        //S2状态，传入基于S2状态可以进行的一系列状态转移
        S2(new Transition("fail",Matched),new Transition("success",Terminal)),

        S1(new Transition("fail",S2),new Transition("success",Terminal)),

        Initial(new Transition("fail",S1),new Transition("success",Terminal)),
        ;
        private Transition[] transitions;

        State(Transition... transitions){

            this.transitions = transitions;
        }

        //状态转移方法
        public State transition(String eventType){

            for (Transition transition : transitions){
                if (transition.getEventType().equals(eventType)){
                    return transition.getTargetState();
                }
            }

            return Initial;
        }

    }

    //状态转移类
    public static class Transition{
        private String eventType;
        private State targetState;

        public Transition(String eventType, State targetState) {
            this.eventType = eventType;
            this.targetState = targetState;
        }

        public Transition() {
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public State getTargetState() {
            return targetState;
        }

        public void setTargetState(State targetState) {
            this.targetState = targetState;
        }
    }
}
