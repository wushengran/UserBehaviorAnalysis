package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
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
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/7 9:04
  */

// 输入及窗口聚合结果样例类
case class WebServerLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)
case class PageViewCount(url: String, count: Long, windowEnd: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
//    val inputStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map( line => {
        val arr = line.split(" ")
        // 从日志数据中提取时间字段，并转换成时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(arr(3)).getTime
        WebServerLogEvent(arr(0), arr(1), timestamp, arr(5), arr(6))
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[WebServerLogEvent](Time.seconds(3)) {
        override def extractTimestamp(element: WebServerLogEvent): Long = element.timestamp
      })

    val lateTag = new OutputTag[WebServerLogEvent]("late")

    // 开窗聚合
    val aggStream = dataStream
      .filter( _.method == "GET" )    // 对GET请求进行过滤
      .keyBy( _.url )    // 按url进行分组
      .timeWindow( Time.minutes(10), Time.seconds(5) )    // 滑动窗口聚合
        .allowedLateness( Time.minutes(1) )
        .sideOutputLateData( lateTag )
      .aggregate( new PageContAgg(), new PageCountWindowResult() )

    dataStream.print("data")
    aggStream.print("agg")
    aggStream.getSideOutput(lateTag).print("late")

    // 排序输出
    val resultStream = aggStream
      .keyBy( _.windowEnd )
      .process( new TopNPageResult(3) )

    resultStream.print()

    env.execute("network flow job")
  }
}

class PageContAgg() extends AggregateFunction[WebServerLogEvent, Long, Long]{
  override def add(value: WebServerLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, input.head, window.getEnd))
  }
}

// 实现自定义ProcessFunction，进行排序输出
class TopNPageResult(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{
//  // 定义一个列表状态
//  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))

  // 为了对于同一个key进行更新操作，定义映射状态
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
//    pageViewCountListState.add(value)

    pageViewCountMapState.put(value.url, value.count)

    // 注册一个定时器，100毫秒后触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    // 注册一个1分钟之后的定时器，用于清理状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    if( timestamp == ctx.getCurrentKey + 60 * 1000L ){
      pageViewCountMapState.clear()
      return
    }

    // 获取状态中的所有窗口聚合结果
//    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
//    val iter = pageViewCountListState.get().iterator()
//    while(iter.hasNext)
//      allPageViewCounts += iter.next()

    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter = pageViewCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

    // 提前清除状态
//     pageViewCountListState.clear()

    // 排序取top n
//    val topNHotPageViewCounts = allPageViewCounts.sortWith( _.count > _.count ).take(n)
    val topNHotPageViewCounts = allPageViewCounts.sortWith( _._2 > _._2 ).take(n)

    // 排名信息格式化打印输出
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append( new Timestamp(timestamp - 100) ).append("\n")
    // 遍历topN 列表，逐个输出
    for( i <- topNHotPageViewCounts.indices ){
      val currentItemViewCount = topNHotPageViewCounts(i)
      result.append("NO.").append( i + 1 ).append(":")
        .append("\t 页面 URL = ").append( currentItemViewCount._1 )
        .append("\t 热门度 = ").append( currentItemViewCount._2 )
        .append("\n")
    }
    result.append("\n =================================== \n\n")

    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}