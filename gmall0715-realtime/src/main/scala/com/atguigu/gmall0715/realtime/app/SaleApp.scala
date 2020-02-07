package com.atguigu.gmall0715.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall0715.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //  1  消费kafka 的数据
    val inputOrderInfoDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
    val inputOrderDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    val inputUserInfoDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER_INFO, ssc)

    // 订单结构的转化
    val orderInfoDstream: DStream[OrderInfo] = inputOrderInfoDstream.map { record =>
      // 获取到的json字符串
      val jsonString: String = record.value()
      println(jsonString)
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      // 对手机号进行脱敏处理
      // tuple是手机号切割后的前后两部分
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
      orderInfo.consignee_tel = telTuple._1 + "********";

      // 进行时间的处理
      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

      orderInfo
    }

    // 订单详情结构转换
    val orderDetailDstream: DStream[OrderDetail] = inputOrderDetailDstream.map { record =>
      val jsonString: String = record.value()
      JSON.parseObject(jsonString, classOf[OrderDetail])
    }

    // 双流join
    // join之前需要先转换为kv类型，以order_id为key
    val orderInfoWithIdDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithIdDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

    val orderFullJoinDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithIdDstream.fullOuterJoin(orderDetailWithIdDstream)

    orderFullJoinDstream.flatMap {
      case (orderId, (orderInfoOption, orderDetailOption)) =>

        val saleDetailList = new ListBuffer[SaleDetail]
        val jedis: Jedis = RedisUtil.getJedisClient

        // TODO 1 先判断主表是否存在
        if (orderInfoOption != None) {
          val orderInfo: OrderInfo = orderInfoOption.get
          // TODO 1.1 判断从表是否存在
          if (orderDetailOption != None) {
            val orderDetail: OrderDetail = orderDetailOption.get
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
          // TODO 1.2 从表不存在，将主表转换为json字符串写入缓存
          // type: string, key: order_info:[orderId] value: orderInfoJson expire
          val orderInfoJson: String = JSON.toJSONString(orderInfo)
          jedis.setex("orderInfo:" + orderInfo.id, 600, orderInfoJson)

          // TODO 1.3 查询缓存中是否有对应的orderDetail
          val orderDetailKey = "orderDetail:" + orderInfo.id
          val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailKey)

        } else {

          //  TODO 2.1 从表查询缓存，获得主表信息
          val orderDetail: OrderDetail = orderDetailOption.get
          val orderInfoKey = "orderInfo:" + orderDetail.order_id
          val orderJson: String = jedis.get(orderInfoKey)

          if (orderJson != null && orderJson.length > 0) {
            val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

          // TODO 2.2 从表保存到redis中
          val orderDetailJson: String = JSON.toJSONString(orderDetail)
          // 1 type : set    key : order_detail:[order_id]  value : order_detail json .....
          val orderDetailKey = "orderDetail:" + orderDetail.order_id
          jedis.sadd(orderDetailKey, orderDetailJson)
          jedis.expire(orderDetailKey, 600)
        }

        saleDetailList
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
