package com.atguigu.newworkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/3 14:22
  */

case class UvCount( windowEnd: Long, count: Long )

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(4)

    val inputStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 进行开窗统计聚合
    val uvCountStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll( Time.hours(1) )    // 统计每小时的uv值
      .apply( new UvCountResult() )

    uvCountStream.print()

    env.execute("uv job")
  }
}

// 自定义的全窗口函数
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 用一个集合来保存所有的userId，实现自动去重
    var idSet = Set[Long]()
    // 遍历所有数据，添加到set中
    for( ub <- input ){
      idSet += ub.userId
    }
    // 包装好样例类类型输出
    out.collect( UvCount(window.getEnd, idSet.size) )
  }
}