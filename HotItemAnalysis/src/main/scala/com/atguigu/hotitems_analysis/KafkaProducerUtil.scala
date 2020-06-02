package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/2 15:21
  */
object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
  def writeToKafka(topic: String): Unit ={
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 创建kafka producer
    val producer = new KafkaProducer[String, String](properties)

    // 读取文件数据，逐行发送到kafka topic
    val bufferSource = io.Source.fromFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for( line <- bufferSource.getLines() ){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
