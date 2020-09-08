package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
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
  * Created by wushengran on 2020/9/8 10:17
  */

// 定义输入输出样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
case class AdViewCountByProvince(windowEnd: String, province: String, count: Long)
// 侧输出流黑名单报警信息样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByProvince {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类
    val resource = getClass.getResource("/AdClickLog.csv")
    val adLogStream: DataStream[AdClickLog] = env.readTextFile(resource.getPath)
      .map( line => {
        val arr = line.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 自定义过滤过程，将超出点击上限的用户输出到侧输出流
    val filteredStream: DataStream[AdClickLog] = adLogStream
      .keyBy( data => (data.userId, data.adId) )    // 以用户和广告id分组
      .process( new FilterBlackListUser(100) )

    // 根据省份分组，开窗聚合
    val adCountStream: DataStream[AdViewCountByProvince] = filteredStream
      .keyBy(_.province)
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate( new AdCountAgg(), new AdCountResult() )

    adCountStream.print("count")
    filteredStream.getSideOutput(new OutputTag[BlackListWarning]("black-list")).print("black list")

    env.execute("ad click count job")
  }
}

// 实现自定义的预聚合函数和窗口函数
class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long]{
  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, AdViewCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdViewCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect( AdViewCountByProvince(end, key, input.head) )
  }
}

// 实现自定义的KeyedProcessFunction
class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]{
  // 定义状态，保存当前用户对当前广告的点击count值，0点清空状态定时器时间戳，是否输出到黑名单的标识位
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timer-ts", classOf[Long]))
  lazy val isInBlackListState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-in-blacklist", classOf[Boolean]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    // 获取count值
    val curCount = countState.value()

    // 判断是否是当天的第一个数据，如果是，注册第二天0点的定时器
    if( curCount == 0 ){
      val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (24*60*60*1000) - 8*60*60*1000
      ctx.timerService().registerProcessingTimeTimer(ts)
      resetTimerTsState.update(ts)
      println(new Timestamp(ts))
    }

    // 过滤过程：判断是否达到了上限，如果达到，那么加入黑名单
    if( curCount >= maxCount ){
      // 如果没有在黑名单里，那么输出到侧输出流黑名单信息中
      if( !isInBlackListState.value() ){
        ctx.output( new OutputTag[BlackListWarning]("black-list"),
          BlackListWarning(value.userId, value.adId, s"click over $maxCount times today") )
        isInBlackListState.update(true)
      }
      return
    }
    out.collect(value)
    countState.update(curCount + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    // 定时器触发时，判断是否为0点定时器，如果是，清空状态
    if( timestamp == resetTimerTsState.value() ){
      countState.clear()
      isInBlackListState.clear()
      resetTimerTsState.clear()
    }
  }
}