package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
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
  * Created by wushengran on 2020/6/6 10:23
  */
object LoginFailPro {
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
      .process( new LoginFailDetectWarningPro() )

    loginFailWarningStream.print()

    env.execute("login fail job")
  }
}

// 自定义Process Function，实现对2次连续登录失败的实时检测
class LoginFailDetectWarningPro() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // 因为只考虑两次登录失败，所以只需要保存上一个登录失败的事件
  lazy val lastLoginFailState: ValueState[LoginEvent] = getRuntimeContext.getState(new ValueStateDescriptor[LoginEvent]("last-loginfail", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 获取上次登录失败事件
    val lastLoginFail = lastLoginFailState.value()

    // 判断当前事件类型，只处理失败事件，如果是成功，状态直接清空
    if( value.eventType == "fail" ){
      // 如果是失败，还需要判断之前是否已经有失败事件
      if( lastLoginFail != null ){
        // 如果已经有第一次失败事件，现在是连续失败，判断时间差
        if( value.timestamp - lastLoginFail.timestamp <= 2 ){
          // 如果是两秒之内，输出报警
          out.collect( LoginFailWarning( value.userId, lastLoginFail.timestamp, value.timestamp, "login fail" ) )
        }
      }
      lastLoginFailState.update(value)
    } else {
      lastLoginFailState.clear()
    }
  }
}