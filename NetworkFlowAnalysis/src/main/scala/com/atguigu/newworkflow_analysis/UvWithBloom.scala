package com.atguigu.newworkflow_analysis

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
  * Package: com.atguigu.newworkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/3 15:44
  */

object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 进行开窗统计聚合
    val uvCountStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1)) // 统计每小时的uv值
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

    uvCountStream.print()

    env.execute("uv with bloom job")
  }
}

// 自定义一个触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
}

// 自定义一个布隆过滤器，位图是在外部redis，这里只保存位图的大小，以及hash函数
class Bloom(size: Long) extends Serializable{
  // 一般取cap是2的整次方
  private val cap = size
  // 实现一个hash函数
  def hash( value: String, seed: Int ): Long ={
    var result = 0L
    for( i <- 0 until value.length ){
      // 用每个字符的ascii码值做叠加计算
      result = result * seed + value.charAt(i)
    }
    // 返回一个cap范围内hash值
    (cap - 1) & result
  }
}

// 自定义处理逻辑，ProcessWindowFunction
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  lazy val jedis = new Jedis("localhost", 6379)
  // 需要处理1亿用户的去重，定义布隆过滤器大小为大约10亿，取2的整次幂就是2^30
  lazy val bloomFilter = new Bloom(1<<30)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 定义在redis中保存的位图的key，以当前窗口的end作为key，（windowEnd，bitmap）
    val storeKey = context.window.getEnd.toString

    // 把当前uv的count值也保存到redis中，保存成一张叫做count的hash表，（windowEnd，uvcount）
    val countMapKey = "count"

    // 初始化操作，从redis的count表中，查到当前窗口的uvcount值
    var count = 0L
    if( jedis.hget(countMapKey, storeKey) != null ){
      count = jedis.hget(countMapKey, storeKey).toLong
    }

    // 开始做去重，首先拿到userId
    val userId = elements.last._2.toString
    // 调用布隆过滤器的hash函数，计算位图中的偏移量
    val offset = bloomFilter.hash(userId, 61)

    // 使用redis命令，查询位图中对应位置是否为1
    val isExist: Boolean = jedis.getbit(storeKey, offset)
    if(!isExist){
      // 如果不存在userId，对应位图位置要置1，count加一
      jedis.setbit(storeKey, offset, true)
      jedis.hset( countMapKey, storeKey, (count + 1).toString )
    }
  }
}