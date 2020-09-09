package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/9 14:26
  */

// 到账事件样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TxMatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取数据
    val orderResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(orderResource.getPath)
      .map( line => {
        val arr = line.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      } )
      .assignAscendingTimestamps( _.timestamp * 1000L )
      .filter(_.txId != "")
//      .keyBy(_.txId)

    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      .map( line => {
        val arr = line.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      } )
      .assignAscendingTimestamps( _.timestamp * 1000L )
//      .keyBy(_.txId)

    // 用connect连接两条流，进行处理
    val resultStream = orderEventStream.connect(receiptEventStream)
      .keyBy(_.txId, _.txId)
      .process( new TxMatchDetect() )

    resultStream.print()
    resultStream.getSideOutput(new OutputTag[OrderEvent]("single-pay")).print("single-pay")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("single-receipt")).print("single-receipt")

    env.execute("tx match job")
  }
}

// 实现自定义的处理函数
class TxMatchDetect() extends KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  // 定义状态，保存当前已到达的pay事件和receipt
  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

  override def processElement1(pay: OrderEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 来的是pay事件，要判断当前是否已有receipt事件到来
    val receipt = receiptEventState.value()
    if( receipt != null ){
      // 已经有到账信息，正常匹配输出到主流
      out.collect((pay, receipt))
      // 清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 如果没有，更新状态，注册定时器等待receipt
      payEventState.update(pay)
      ctx.timerService().registerEventTimeTimer( pay.timestamp * 1000L + 5000L )    // 等待5秒钟
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 来的是receipt事件，要判断当前是否已有pay事件到来
    val pay = payEventState.value()
    if( pay != null ){
      // 已经有到账信息，正常匹配输出到主流
      out.collect((pay, receipt))
      // 清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 如果没有，更新状态，注册定时器等待receipt
      receiptEventState.update(receipt)
      ctx.timerService().registerEventTimeTimer( receipt.timestamp * 1000L + 3000L )    // 等待3秒钟
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 定时器触发，判断是否有某一个状态不为空，如果不为空，就是另一个没来
    if( payEventState.value() != null ){
      ctx.output(new OutputTag[OrderEvent]("single-pay"), payEventState.value())
    }
    if( receiptEventState.value() != null ){
      ctx.output(new OutputTag[ReceiptEvent]("single-receipt"), receiptEventState.value())
    }
    // 清空状态
    payEventState.clear()
    receiptEventState.clear()
  }
}