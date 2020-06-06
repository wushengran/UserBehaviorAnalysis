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
  * Created by wushengran on 2020/6/5 15:24
  */

// 输入输出样例类
case class LoginEvent( userId: Long, ip: String, eventType: String, timestamp: Long )
case class LoginFailWarning( userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String )

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // 自定义ProcessFunction，通过注册定时器实现判断2s内连续登录失败的需求
    val loginFailWarningStream: DataStream[LoginFailWarning] = loginEventStream
      .keyBy(_.userId)     // 按照用户id分组检测
      .process( new LoginFailDetectWarning(2) )

    loginFailWarningStream.print()

    env.execute("login fail job")
  }
}

// 实现自定义的KeyedProcessFunction
class LoginFailDetectWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // 定义状态，用来保存所有的登录失败事件，以及注册的定时器时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 每来一个数据，判断当前登录事件是成功还是失败
    if( value.eventType == "fail" ){
      // 如果是失败，保存到ListState里，还需要判断是否应该注册定时器
      loginFailListState.add(value)
      if( timerTsState.value() == 0 ){
        // 如果没有定时器，就注册一个2秒后的
        val ts = value.timestamp * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      // 如果是成功，删除定时器，清空状态，重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    // 定时器触发时，说明没有成功事件来，统计所有的失败事件个数，如果大于设定值就报警
    import scala.collection.JavaConversions._
    val loginFailList = loginFailListState.get().toList

    if( loginFailList.length >= maxFailTimes ){
      out.collect( LoginFailWarning(
        ctx.getCurrentKey,
        loginFailList.head.timestamp,
        loginFailList.last.timestamp,
        "login fail in 2s for " + loginFailList.length + " times."
      ) )
    }
    // 清空状态
    loginFailListState.clear()
    timerTsState.clear()
  }
}