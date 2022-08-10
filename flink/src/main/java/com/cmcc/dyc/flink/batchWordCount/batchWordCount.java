package com.cmcc.dyc.flink.batchWordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class batchWordCount {
    public static void main(String[] args) throws Exception {
        String inputPath = "";
        String outputPath = "";

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //获取文件中的内容
        DataSource<String> text = env.readTextFile(inputPath);

        //groupBy(num) :按照第几列进行排序；sum(num):排序后将第二列的值进行求和
        DataSet<Tuple2<String,Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsCsv(outputPath,"\n"," ").setParallelism(1);
        env.execute("batch word count");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //\w 匹配字母或数字或下划线或汉字  等价于[^A-Za-z0-9_]
            //\W 非数字字母下划线
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token: tokens) {
                if (token.length() > 0) {
                    //转变成 word   1的格式。每个新的单词字数都是1
                    out.collect(new Tuple2<String,Integer>(token,1));
                }
            }
        }
    }

}
