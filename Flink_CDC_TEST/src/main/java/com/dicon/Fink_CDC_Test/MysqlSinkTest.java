package com.dicon.Fink_CDC_Test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.logging.LogManager;

public class MysqlSinkTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("slave2", 9999);

        DataStream<Tuple2<String,String>> map = dataStreamSource.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(String s) throws Exception {
                String[] words = s.split(" ");
                System.out.println(Arrays.toString(words));
                return new Tuple2<String,String>(words[0],words[1]);
            }
        });

        map.addSink(JdbcSink.sink(
                "Insert into `user`.`log_info_test` (user_account,login_time) values(?,?);"
                ,(ps, msgInfo) -> {
                    ps.setString(1, msgInfo.f0);
                    ps.setString(2, msgInfo.f1);

                },
                new JdbcExecutionOptions.Builder().withBatchSize(NumberUtils.createInteger("1")).build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.70.48:3306/user?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true")
                        .withUsername("root")
                        .withPassword("123456")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .build()
        ));

        env.execute("MysqlSinkTest");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MsgInfoTest{

        private String loginTime;

        private String userAccount;

        private String clickUrl;



    }

}
