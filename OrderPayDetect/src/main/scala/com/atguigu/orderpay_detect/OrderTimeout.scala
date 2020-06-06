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
  * Created by wushengran on 2020/6/6 15:34
  */

// 定义输入输出样例类
case class OrderEvent( orderId: Long, eventType: String, txId: String, timestamp: Long )
case class OrderResult( orderId: Long, status: String )

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取数据转换成样例类
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream: DataStream[OrderEvent] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 2. 定义一个pattern
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 3. 将pattern应用到按照orderId分组后的datastream上
    val patternStream = CEP.pattern( orderEventStream.keyBy(_.orderId), orderPayPattern )

    // 4. 定义一个超时订单侧输出流的标签
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")

    // 5. 调用select方法，分别处理匹配数据和超时未匹配数据
    val resultStream: DataStream[OrderResult] = patternStream
      .select( orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect() )

    // 6. 打印输出
    resultStream.print("pay")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}

// 实现自定义的PatternTimeoutFunction
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    print(timeoutTimestamp)
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

// 实现自定义的PatternSelectFunction
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderResult( payedOrderId, "payed" )
  }
}