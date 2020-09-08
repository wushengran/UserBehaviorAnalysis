package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/8 14:38
  */
object LoginFailPro {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map( line => {
        val arr = line.split(",")
        UserLoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: UserLoginEvent): Long = element.timestamp * 1000L
      })

    // 用process function实现连续登录失败的检测
    val loginFailWarningStream = loginEventStream
      .keyBy( _.userId )    // 基于userId分组
      .process( new LoginFailDetectProWarning() )

    loginFailWarningStream.print()
    env.execute("login fail pro job")
  }
}

class LoginFailDetectProWarning() extends KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]{
  // 定义状态，保存所有登录失败事件的列表
  lazy val loginFailEventListState: ListState[UserLoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("fail-list", classOf[UserLoginEvent]))

  override def processElement(value: UserLoginEvent, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 判断当前数据是登录成功还是失败
    if( value.eventType == "fail" ){
      // 1. 如果是失败，判断之前是否已有登录失败事件
      val iter = loginFailEventListState.get().iterator()
      if( iter.hasNext ){
        // 1.1 如果已有登录失败，继续判断是否在2秒之内
        val firstFailEvent = iter.next()
        if( value.timestamp - firstFailEvent.timestamp <= 2 ){
          // 在2秒之内，输出报警
          out.collect( LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail in 2s for 2 times") )
        }
        // 不管报不报警，直接清空状态，将最近一次失败数据添加进去
        loginFailEventListState.clear()
        loginFailEventListState.add(value)
      } else {
        // 1.2 如果没有数据，当前是第一次登录失败，直接添加到状态列表
        loginFailEventListState.add(value)
      }
    } else {
      // 2. 如果是成功，清空状态，重新开始
      loginFailEventListState.clear()
    }
  }
}