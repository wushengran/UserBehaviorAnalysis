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
  * Created by wushengran on 2020/6/5 11:32
  */

// 定义输入输出样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
case class AdCountViewByProvince( windowEnd: String, province: String, count: Long )
// 侧输出流的黑名单报警信息样例类
case class BlackListWarning( userId: Long, adId: Long, msg: String )

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

    // 自定义一个ProcessFunction，实现刷单行为的检测和过滤
    val filterBlackListStream: DataStream[AdClickEvent] = adEventStream
      .keyBy( data => (data.userId, data.adId) )    // 根据用户id和广告id分组，统计count值
      .process( new FilterBlackListUser(100) )

    // 做开窗统计，得到聚合结果
    val adCountStream: DataStream[AdCountViewByProvince] = filterBlackListStream
      .keyBy(_.province)     // 按照省份分组统计
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate( new AdCountAgg(), new AdCountByProvinceResult() )

    adCountStream.print("count")
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("warning")

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

// 实现自定义ProcessFunction，保存当前用户对广告的点击量count，判断是否超过上限
class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
  // 定义状态，保存当前用户对广告的点击量，当前用户是否已被输出到了侧输出流黑名单，0点清空状态的定时器时间戳
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent", classOf[Boolean]))
  lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timerTs", classOf[Long]))

  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
    // 获取当前的count值，进行判断
    val curCount = countState.value()
    // 如果是当天的第一次处理，注册一个定时器，第二天0点清除所有状态，重新开始
    if( curCount == 0 ){
      val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (24*60*60*1000) - 8*60*60*1000
      println(new Timestamp(ctx.timerService().currentProcessingTime()))
      println(new Timestamp(ts))
      ctx.timerService().registerProcessingTimeTimer(ts)
      resetTimerTsState.update(ts)
    }
    // 如果count已经超过上限，那么就过滤掉；如果没有输出过黑名单信息，那么就报警
     if(curCount >= maxCount){
       if( !isSentState.value() ){
         ctx.output(new OutputTag[BlackListWarning]("blacklist"), BlackListWarning(value.userId, value.adId, "click over " + maxCount + " times today."))
         isSentState.update(true)
       }
       return
     }
    // 如果正常，那么直接输出到主流里做开窗统计
    out.collect( value )
    countState.update(curCount + 1)
  }

  // 定时器触发时，判断是否是resetTimer，清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
    if( timestamp == resetTimerTsState.value() ){
      isSentState.clear()
      countState.clear()
      resetTimerTsState.clear()
    }
  }
}