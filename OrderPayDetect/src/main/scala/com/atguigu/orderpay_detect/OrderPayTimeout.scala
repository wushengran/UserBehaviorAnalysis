package com.atguigu.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/9 9:00
  */

// 定义输入输出样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)
case class OrderPayResult(orderId: Long, resultMsg: String)

object OrderPayTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map( line => {
        val arr = line.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      } )
      .assignAscendingTimestamps( _.timestamp * 1000L )

    // 1. 定义一个匹配模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2. 将模式应用在数据流上，进行复杂事件序列的检测
    val patternStream = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

    // 3. 定义一个侧输出流标签，用于将超时事件输出
    val timeoutOutputTag = new OutputTag[OrderPayResult]("timeout")

    // 4. 检出复杂事件，并转换输出结果
    val resultStream = patternStream.select( timeoutOutputTag, new OrderPayTimeoutSelect(), new OrderPaySelect() )

    resultStream.print("payed")
    resultStream.getSideOutput(timeoutOutputTag).print("timeout")

    env.execute("order pay timeout job")
  }
}

// 自定义PatternSelectFunction
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderPayResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderPayResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderPayResult(payedOrderId, "payed successfully")
  }
}

// 自定义PatternTimeoutFunction
class OrderPayTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderPayResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderPayResult = {
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderPayResult(timeoutOrderId, s"order timeout at $timeoutTimestamp")
  }
}