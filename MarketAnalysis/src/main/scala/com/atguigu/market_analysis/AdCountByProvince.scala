package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.market_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/5 11:32
  */

// 定义输入输出样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
case class AdCountViewByProvince( windowEnd: String, province: String, count: Long )

object AdCountByProvince {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件读取数据，map成样例类，并提取时间戳和watermark
    val resource = getClass.getResource("/AdClickLog.csv")
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 做开窗统计，得到聚合结果
    val adCountStream: DataStream[AdCountViewByProvince] = adEventStream
      .keyBy(_.province)     // 按照省份分组统计
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate( new AdCountAgg(), new AdCountByProvinceResult() )

    adCountStream.print()

    env.execute("ad count by province job")
  }
}

// 实现自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 实现自定义的窗口函数，主要是提取窗口信息，包装成样例类
class AdCountByProvinceResult() extends WindowFunction[Long, AdCountViewByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountViewByProvince]): Unit = {
    val windowEnd = new Timestamp( window.getEnd ).toString
    out.collect( AdCountViewByProvince(windowEnd, key, input.iterator.next()) )
  }
}