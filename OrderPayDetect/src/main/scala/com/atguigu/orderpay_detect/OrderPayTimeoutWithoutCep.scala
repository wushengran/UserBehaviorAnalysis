package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/9 11:24
  */
object OrderPayTimeoutWithoutCep {
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

    // 直接自定义ProcessFunction，检测不同的订单支付情况
    val orderResultStream = orderEventStream
      .keyBy(_.orderId)
      .process( new OrderPayDetect() )

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("payed-but-timeout")).print("payed-but-timeout")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("data not found")).print("data not found")
    orderResultStream.getSideOutput(new OutputTag[OrderPayResult]("timeout")).print("timeout")

    env.execute("order pay timeout without cep job")
  }
}

// 实现自定义的订单支付检测流程
class OrderPayDetect() extends KeyedProcessFunction[Long, OrderEvent, OrderPayResult]{
  // 定义状态，用来保存是否来过create、pay事件，保存定时器事件戳
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#Context, out: Collector[OrderPayResult]): Unit = {
    // 先拿到当前状态
    val isPayed = isPayedState.value()
    val isCreated = isCreatedState.value()
    val timerTs = timerTsState.value()

    // 判断当前事件的类型
    if( value.eventType == "create" ){
      // 1. 如果来的是create，要判断之前是否pay过
      if( isPayed ){
        // 1.1 如果已经支付过，匹配成功，输出到主流
        out.collect(OrderPayResult(value.orderId, "payed successfully"))
        // 清除定时器，清空状态
        ctx.timerService().deleteEventTimeTimer(timerTs)
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
      } else{
        // 1.2 如果pay没来过，最正常的情况，注册15分钟后的定时器，开始等待
        val ts = value.timestamp * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        // 更新状态
        timerTsState.update(ts)
        isCreatedState.update(true)
      }
    } else if( value.eventType == "pay" ){
      // 2. 来的是pay，判断是否create过
      if( isCreated ){
        // 2.1 已经create过，匹配成功，还要再确认一下pay时间是否超过了定时器时间
        if( value.timestamp * 1000L < timerTs ){
          // 2.1.1 没有超时，正常输出到主流
          out.collect(OrderPayResult(value.orderId, "payed successfully"))
        } else {
          // 2.1.2 已经超时，但因为乱序数据的到来，定时器还没触发，输出到侧输出流
          ctx.output( new OutputTag[OrderPayResult]("payed-but-timeout"), OrderPayResult(value.orderId, "payed but already timeout") )
        }
        // 已经处理完当前订单状态，清空状态和定时器
        ctx.timerService().deleteEventTimeTimer(timerTs)
        isCreatedState.clear()
        isPayedState.clear()
        timerTsState.clear()
      } else {
        // 2.2 如果没有create过，乱序，注册定时器等待create
        ctx.timerService().registerEventTimeTimer( value.timestamp * 1000L )
        // 更新状态
        timerTsState.update(value.timestamp * 1000L)
        isPayedState.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderPayResult]#OnTimerContext, out: Collector[OrderPayResult]): Unit = {
    // 定时器触发，说明create和pay有一个没来
    if( isPayedState.value() ){
      // 如果pay过，说明create没来
      ctx.output(new OutputTag[OrderPayResult]("data not found"), OrderPayResult(ctx.getCurrentKey, "payed but not found create"))
    } else{
      // 没有pay过，真正超时
      ctx.output(new OutputTag[OrderPayResult]("timeout"), OrderPayResult(ctx.getCurrentKey, "timeout"))
    }
    // 清空状态
    isCreatedState.clear()
    isPayedState.clear()
    timerTsState.clear()
  }
}