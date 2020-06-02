package com.atguigu.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/2 15:34
  */
object HotItemsWithTableApi {
  def main(args: Array[String]): Unit = {
    // 创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 创建表执行环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
    // 转换成样例类类型，并分配时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 把流转换成表，并提取需要的字段，定义时间属性
    val dataTable = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 进行开窗聚合操作，调用table API
    val aggTable = dataTable
      .filter( 'behavior === "pv" )
      .window( Slide over 1.hours every 5.minutes on 'ts as 'sw )
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)

    // 将聚合结果表注册到环境，写SQL实现top N 的选取
    tableEnv.createTemporaryView("agg", aggTable, 'itemId, 'cnt, 'windowEnd)
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select *, row_number() over (partition by windowEnd order by cnt desc) as row_num
        |  from agg
        |  )
        |where row_num <= 5
      """.stripMargin)

    aggTable.toAppendStream[Row].print("agg")
    resultTable.toRetractStream[Row].print("result")

    env.execute("hot items with table api job")
  }
}
