package com.dicon.Fink_CDC_Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Mysql -> FlinkCDC ->Kafka
 *
 * @author dyc
 * @date 2023-06-13
 *
 */
public class Mysql2Kafka {

    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO addsource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.70.48")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("usercontrol")
                .tableList("usercontrol.log_info")
                //初始化全量
                //.startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

//        env.enableCheckpointing(3000);

        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql Source")
                .setParallelism(4);

        OutputTag<MsgInfo> MysqlTag = new OutputTag<MsgInfo>("MysqlTag"){};
        OutputTag<MsgInfo> STDTag = new OutputTag<MsgInfo>("STDTag"){};

        SingleOutputStreamOperator<MsgInfo> process = dataStreamSource.map(new MapFunction<String, MsgInfo>() {
            @Override
            public MsgInfo map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String data = jsonObject.getString("after");
                JSONObject jsonObject1 = JSON.parseObject(data);

                //TODO yyyy-MM-dd'T'HH:mm:ss'Z' -> String "yyyy-MM-dd HH:mm:ss"
                String dateString = jsonObject1.getString("login_time");
                SimpleDateFormat formatUTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                formatUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date date = formatUTC.parse(dateString);
                SimpleDateFormat formatDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String loginDate = formatDateTime.format(date).toString();

                //TODO 构造并返回MsgInfo实体类
                return new MsgInfo(loginDate.toString(), jsonObject1.getString("user_account"), jsonObject1.getString("click_url"));
            }
        }).process(new ProcessFunction<MsgInfo, MsgInfo>() {
            @Override
            public void processElement(MsgInfo value, ProcessFunction<MsgInfo, MsgInfo>.Context ctx, Collector<MsgInfo> out) throws Exception {
                out.collect(value);
                ctx.output(MysqlTag, value);
                ctx.output(STDTag, value);
            }
        });

//        //        //TODO addSink
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                        .setBootstrapServers("master:9092,slave1:9092,slave2:9092")
//                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                                .setTopic("")
//                                .setValueSerializationSchema(new SimpleStringSchema())
//                                .build()
//                        )
//                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                        .build();
//
//        dataStreamSource.sinkTo(sink);

        process.getSideOutput(MysqlTag).addSink(JdbcSink.sink(
                "Insert into `user`.`log_info` (user_account,login_time,click_url) values(?,?,?)"
                ,(ps, msgInfo) -> {
                    ps.setString(1, msgInfo.getUserAccount());
                    ps.setString(2, msgInfo.getLoginTime());
                    ps.setString(3, msgInfo.getClickUrl());

                },
                new JdbcExecutionOptions.Builder().withBatchSize(NumberUtils.createInteger("1")).build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.56.104:3306/user?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true")
                        .withUsername("root")
                        .withPassword("123456")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .build()
        ));

        process.getSideOutput(STDTag).print();

        env.execute("Mysql2Kafka");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MsgInfo{

        private String loginTime;

        private String userAccount;

        private String clickUrl;



    }

}


