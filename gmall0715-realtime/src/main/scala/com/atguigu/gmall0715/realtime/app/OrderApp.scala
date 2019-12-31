package com.atguigu.gmall0715.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtime.bean.OrderInfo
import com.atguigu.gmall0715.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._

object OrderApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // 从kafka中读取数据
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)


    // 对数据进行结构的转换
    val orderInfoDStream: DStream[OrderInfo] = inputDstream.map(record => {
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
    })

    // orderInfoDstream    // 增加一个 字段 ： 是否是该用户首次下单的标志  is_first_order
//    orderInfoDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition{orderInfoItr=>{
//        val jedis: Jedis = RedisUtil.getJedisClient
//
//        for (orderInfo <- orderInfoItr) {
//          jedis.sadd("order", orderInfo.consignee)
//          jedis.expire("order", 2 * 24 * 3600) // 设置两天后过期
//        }
//        jedis.close()
//      }}
//    })


    orderInfoDStream.foreachRDD{rdd=>
//      println("==============")
      rdd.saveToPhoenix("GMALL0715_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(),Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    ssc.start()
    ssc.awaitTermination()


  }
}
