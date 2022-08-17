package com.zyjk.dicon.flink.MyJarTest;

/**
 * author:diaoyachong
 * date:2022-08-16
 * func:flink上传jar包测试
 */

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;



public class MyJarTest {

    public static void main(String[] args) throws Exception {

        //创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //路径
        if (args[0]!=null && args[1]!=null){
            String InputPath = args[0];
            String OutputPath = args[1];
            //读取数据
            DataSource<String> InputData = env.readTextFile(InputPath);

            //对数据进行处理
            DataSet<Tuple2<String,Integer>> OutputData = InputData.flatMap(new Tokenizer()).groupBy(0).sum(1);

            OutputData.writeAsCsv(OutputPath,"\n"," ").setParallelism(1);

            env.execute("MybatchWordCount");
        }else {
            return;
        }



    }


}

