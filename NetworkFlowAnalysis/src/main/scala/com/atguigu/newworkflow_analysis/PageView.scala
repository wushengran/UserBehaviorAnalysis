package com.atguigu.newworkflow_analysis

import java.lang

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/3 11:37
  */

// 定义输入输出样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class PvCount(windowEnd: Long, count: Long)

object PageView {
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
    val pvCountStream: DataStream[PvCount] = dataStream
      .filter(_.behavior == "pv")
//      .map( data => ("pv", 1L) )    // map成二元组，用一个哑key来作为分组的key
      .map( new MyMapper() )
      .keyBy(_._1)
      .timeWindow( Time.hours(1) )    // 统计每小时的pv值
      .aggregate( new PvCountAgg(), new PvCountWindowResult() )

    // 把每个key对应的pv count值合并
    val totalPvCountStream: DataStream[PvCount] = pvCountStream
      .keyBy(_.windowEnd)
      .process( new TotalPvCount() )

    totalPvCountStream.print()

    env.execute("pv job")
  }
}

// 自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数
class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}

// 实现自定义的Mapper
class MyMapper() extends MapFunction[UserBehavior, (String, Long)]{
  override def map(value: UserBehavior): (String, Long) = {
    (Random.nextString(4), 1L)
  }
}

// 自定义的合并各key统计结果的ProcessFunction
class TotalPvCount() extends KeyedProcessFunction[Long, PvCount, PvCount]{
  // 定义一个状态，用来保存当前已有的key的count值总计
  lazy val totalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
    val currentTotalCount = totalCountState.value()
    totalCountState.update( currentTotalCount + value.count )
    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    // 定时器触发时，直接输出当前的totalcount
    out.collect( PvCount(ctx.getCurrentKey, totalCountState.value()) )
    // 清空状态
    totalCountState.clear()
  }
}