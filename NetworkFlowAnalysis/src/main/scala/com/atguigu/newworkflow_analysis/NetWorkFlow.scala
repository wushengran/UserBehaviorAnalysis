package com.atguigu.newworkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/3 9:16
  */

// 定义样例类
case class ApacheLogEvent( ip: String, userId: String, eventTime: Long, method: String, url: String )
case class PageViewCount( url: String, windowEnd: Long, count: Long )

object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map( data => {
        val dataArray = data.split(" ")
        // 将时间String转换成Long的时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6))
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })

    val aggStream = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)    // 按照页面url做分组开窗聚合统计
      .timeWindow( Time.minutes(10), Time.seconds(5) )
        .allowedLateness(Time.minutes(1))
        .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate( new PageCountAgg(), new PageCountWindowResult() )

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process( new TopNHotPages(3) )

    dataStream.print("data")
    aggStream.print("agg")
    resultStream.print("result")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")

    env.execute("hot page job")
  }
}

// 自定义预聚合函数
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数
class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect( PageViewCount(key, window.getEnd, input.iterator.next()) )
  }
}

// 自定义KeyedProcessFunction，实现count结果的排序
class TopNHotPages(topSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{
  // 定义列表状态，用来保存当前窗口的所有page的count值
//  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageview-count", classOf[PageViewCount]))

  // 改进：定义MapState，用来保存当前窗口所有page的count值，有更新操作时直接put
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageview-count", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
//    pageViewCountListState.add(value)
    pageViewCountMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    // 定义1分钟之后的定时器，用于清除状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 判断时间戳，如果是1分钟后的定时器，直接清空状态
    if( timestamp == ctx.getCurrentKey + 60 * 1000L ){
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
//    val iter = pageViewCountListState.get().iterator()
//    while(iter.hasNext)
//      allPageViewCounts += iter.next()
    val iter = pageViewCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += PageViewCount(entry.getKey, timestamp - 1, entry.getValue)
    }

//    pageViewCountListState.clear()

    // 将所有count值排序取前 N 个
    val sortedPageViewCounts = allPageViewCounts.sortWith(_.count > _.count).take(topSize)

    val result: StringBuilder = new StringBuilder
    result.append("==================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for(i <- sortedPageViewCounts.indices){
      val currentViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i+1).append(":")
        .append(" 页面url=").append(currentViewCount.url)
        .append(" 访问量=").append(currentViewCount.count)
        .append("\n")
    }
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}