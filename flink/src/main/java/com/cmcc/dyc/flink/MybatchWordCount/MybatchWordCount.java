package com.cmcc.dyc.flink.MybatchWordCount;

import jdk.internal.util.xml.impl.Input;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class MybatchWordCount {

    public static void main(String[] args) throws Exception {

        //创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //路径
        String InputPath = "D:\\myproject\\flink\\flink\\src\\inputdata\\MybatchWordCount\\inputdata.txt";
        String OutputPath = "D:\\myproject\\flink\\flink\\src\\outputdata\\MybatchWordCount\\outputdata";

        //读取数据
        DataSource<String> InputData = env.readTextFile(InputPath);

        //对数据进行处理
        DataSet<Tuple2<String,Integer>> OutputData = InputData.flatMap(new Tokenizer()).groupBy(0).sum(1);

        OutputData.writeAsCsv(OutputPath,"\n"," ").setParallelism(1);

        env.execute("MybatchWordCount");

    }

    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>> {

        @Override
        public void flatMap(String in, Collector<Tuple2<String,Integer>> out) throws Exception {

            //获取每行数据，拆分为单个单词
            String[] words = in.toLowerCase().split("\\W+");

            System.out.println(in);

            //将每个单词转化为（word，1）的格式
            for (String word : words) {

                out.collect(new Tuple2<String,Integer>(word,1));
            }

        }

    }
}
