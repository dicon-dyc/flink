package com.dicon.flink.scala.FlinkScalaFunTest

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}


/**
 * @Author:dyc
 * @Date:2023/04/24
 */

object FlinkScalaFuncTest {


  def main(args: Array[String]): Unit = {


    //TODO 配置环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //TODO 创建source
    val dataStreamSource = environment.socketTextStream("192.168.56.104", 9999)

    //TODO transform
    val resultData = dataStreamSource.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)


    //TODO sink
    resultData.print()

    environment.execute("FlinkScalaFuncTest")

  }

}
