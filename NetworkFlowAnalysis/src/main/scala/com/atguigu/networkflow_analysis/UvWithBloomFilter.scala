package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/7 15:02
  */
object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val uvStream = dataStream
      .filter( _.behavior == "pv" )
      .map( data => ("uv", data.userId) )    // map成二元组，只需要userId，还有dummy key
      .keyBy( _._1 )
      .timeWindow( Time.hours(1) )
      .trigger( new MyTrigger() )    // 自定义窗口触发规则
      .process( new UvCountResultWithBloom() )

    uvStream.print()
    env.execute("uv with bloom job")
  }
}

// 实现自定义触发器，每个数据来了之后都触发一次窗口计算
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
}

// 实现自定义的布隆过滤器
class MyBloomFilter(size: Long) extends Serializable {
  // 指定布隆过滤器的位图大小由外部参数指定（2的整次幂），位图存在redis中
  private val cap = size

  // 实现一个hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for( i <- 0 until value.length ){
      result = result * seed + value.charAt(i)
    }
    // 返回cap范围内的hash值
    (cap - 1) & result
  }
}

// 自定义ProcessFunction，实现对于每个userId的去重判断
class UvCountResultWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  // 创建redis连接和布隆过滤器
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new MyBloomFilter(1 << 29)    // 1亿id，1亿个位大约需要2^27，设置大小为2^29

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 每来一个数据，就将它的userId进行hash运算，到redis的位图中判断是否存在
    val bitmapKey = context.window.getEnd.toString    // 以windowEnd作为位图的key

    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)    // 调用hash函数，计算位图中偏移量
    val isExist = jedis.getbit( bitmapKey, offset )    // 调用redis位命令，得到是否存在的结果

    // 如果存在，什么都不做；如果不存在，将对应位置1，count值加1
    // 因为窗口状态要清空，所以将count值保存到redis中
    val countMap = "uvCount"    // 所有窗口的uv count值保存成一个hash map
    val uvCountKey = bitmapKey    // 每个窗口的uv count key就是windowEnd
    var count = 0L
    // 先取出redis中的count状态
    if( jedis.hget(countMap, uvCountKey) != null )
      count = jedis.hget(countMap, uvCountKey).toLong

    if( !isExist ){
      jedis.setbit(bitmapKey, offset, true)
      // 更新窗口的uv count统计值
      jedis.hset(countMap, uvCountKey, (count + 1).toString)
      out.collect( UvCount(uvCountKey.toLong, count + 1) )
    }
  }
}