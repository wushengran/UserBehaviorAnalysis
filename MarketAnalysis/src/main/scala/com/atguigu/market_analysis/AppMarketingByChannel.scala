package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.market_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/5 10:34
  */

// 输入的样例类类型
case class MarketingUserBehavior( userId: String, channel: String, behavior: String, timestamp: Long )
// 输出的统计结果样例类
case class MarketingByChannelCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long )

// 定义一个自定义的模拟测试源
class SimulatedEventSource() extends RichParallelSourceFunction[MarketingUserBehavior]{
  // 定义是否运行的标识位
  var running = true

  // 定义渠道和用户行为的集合
  val channelSet: Seq[String] = Seq("app-store", "huawei-store", "weibo", "wechat")
  val behaviorSet: Seq[String] = Seq("click", "download", "install", "uninstall")
  val rand: Random = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义最大的数据数量，用于控制测试规模
    val maxCounts = Long.MaxValue
    var count = 0L

    // 无限循环，随机生成所有数据
    while( running && count < maxCounts ){
      val id = UUID.randomUUID().toString
      val channel = channelSet(rand.nextInt(channelSet.size))
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val ts = System.currentTimeMillis()

      // 使用ctx发出数据
      ctx.collect( MarketingUserBehavior(id, channel, behavior, ts) )

      count += 1
      Thread.sleep(10L)
    }
  }
}

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从自定义数据源读取数据进行处理
    val dataStream: DataStream[MarketingUserBehavior] = env.addSource( new SimulatedEventSource() )
      .assignAscendingTimestamps(_.timestamp)

    // 开窗统计，得到渠道的聚合结果
    val resultStream: DataStream[MarketingByChannelCount] = dataStream
      .filter(_.behavior != "uninstall")     // 过滤掉卸载行为
//      .keyBy("channel", "behavior")
      .keyBy( data => (data.channel, data.behavior) )    // 以(channel, behavior)二元组作为分组的key
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .process( new MarketingByChannelCountResult() )

    resultStream.print()

    env.execute("app marketing by channel job")
  }
}

class MarketingByChannelCountResult() extends ProcessWindowFunction[MarketingUserBehavior, MarketingByChannelCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingByChannelCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect( MarketingByChannelCount(start, end, channel, behavior, count) )
  }
}