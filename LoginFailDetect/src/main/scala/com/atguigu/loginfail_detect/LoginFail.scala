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
  * Created by wushengran on 2020/9/8 11:48
  */

// 定义输入输出样例类
case class UserLoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String)

object LoginFail {
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
      .process( new LoginFailDetectWarning(3) )

    loginFailWarningStream.print()
    env.execute("login fail job")
  }
}

class LoginFailDetectWarning(loginFailTimes: Int) extends KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]{
  // 定义状态，保存2秒内所有登录失败事件的列表，定时器时间戳
  lazy val loginFailEventListState: ListState[UserLoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[UserLoginEvent]("fail-list", classOf[UserLoginEvent]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: UserLoginEvent, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 判断当前数据是登录成功还是失败
    if( value.eventType == "fail" ){
      // 1. 如果是失败，添加数据到列表状态中
      loginFailEventListState.add(value)
      // 如果没有定时器，注册一个
      if( timerTsState.value() == 0 ){
        val ts = value.timestamp * 1000L + 5000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      // 2. 如果是成功，删除定时器，清空状态，重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailEventListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UserLoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    import scala.collection.JavaConversions._
    // 定时器触发，说明没有登录成功数据，要判断一共有多少次登录失败
    val loginFailList = loginFailEventListState.get()
    if( loginFailList.size >= loginFailTimes ){
      // 超过了失败上限，输出报警信息
      out.collect(
        LoginFailWarning(
          ctx.getCurrentKey,
          loginFailList.head.timestamp,
          loginFailList.last.timestamp,
          s"login fail in 2s for ${loginFailList.size} times"
      ) )
    }
    // 清空状态
    loginFailEventListState.clear()
    timerTsState.clear()
  }
}
