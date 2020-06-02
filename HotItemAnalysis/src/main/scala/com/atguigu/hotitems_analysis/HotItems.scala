package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/2 11:37
  */

// 输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
// 中间聚合结果样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // 创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件读取数据
//    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    // 从kafka读取数据
    val inputStream: DataStream[String] = env.addSource( new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties) )

    // 转换成样例类类型，并分配时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 进行开窗聚合转换
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")    // 过滤出pv行为，用于热门度的count统计
      .keyBy("itemId")     // 按商品id做分组
      .timeWindow( Time.hours(1), Time.minutes(5) )    // 定义滑动事件窗口
      .aggregate( new CountAgg(), new WindowResult() )

    // 对统计聚合结果按照窗口分组，排序输出
    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd")    // 按窗口结束时间分组
      .process( new TopNHotItems(5) )

    resultStream.print()

    env.execute("hot items job")
  }
}

// 自定义预聚合函数，每来一个数据就count加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义一个全窗口函数，将窗口信息包装进去输出
class WindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

// 自定义一个KeyedProcessFunction，对每个窗口的count统计值排序，并格式化成字符串输出
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{
  // 定义一个列表状态，用来保存当前窗口的所有商品的count值
  private var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-liststate", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，就将它添加到ListState里
    itemViewCountListState.add(value)
    // 需要注册一个windowEnd+1的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 当定时器触发时，当前窗口所有商品的统计数都到齐了，可以直接排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 遍历ListState的数据，全部放到一个ListBuffer中，方便排序
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for( itemViewCount <- itemViewCountListState.get() ){
      allItemViewCounts += itemViewCount
    }

    // 提前清空状态
    itemViewCountListState.clear()

    // 按照count大小排序并取前topSize
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排序数据包装成可视化的String，便于打印输出
    val result: StringBuilder = new StringBuilder
    result.append("==================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 遍历排序结果数组，将每个ItemViewCount的商品ID和count值，以及排名输出
    for(i <- sortedItemViewCounts.indices){
      val currentViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i+1).append(":")
        .append(" 商品ID=").append(currentViewCount.itemId)
        .append(" 点击量=").append(currentViewCount.count)
        .append("\n")
    }
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}