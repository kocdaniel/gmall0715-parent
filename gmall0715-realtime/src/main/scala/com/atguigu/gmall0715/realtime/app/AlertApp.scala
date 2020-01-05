package com.atguigu.gmall0715.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtime.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.gmall0715.realtime.util.{MyESUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

//需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。
// 达到以上要求则产生一条预警日志。
//同一设备，每分钟只记录一次预警。


object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //    需求拆解：
    //    	同一设备
    //    	5分钟内
    //    	三次不同账号登录
    //    	领取优惠券
    //    	没有浏览商品
    //    	同一设备每分钟只记录一次预警


    // 消费kafka的数据
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)

    // 对输入数据进行类型的转换
    val eventInfoDStream: DStream[EventInfo] = inputDStream.map { record => {
      val jsonString: String = record.value()

      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])

      val formattor = new SimpleDateFormat("yyyy-MM-dd HH")

      val dateTime = new Date(eventInfo.ts)

      val dateTimeStr: String = formattor.format(dateTime)

      eventInfo.logDate = dateTimeStr.split(" ")(0)
      eventInfo.logHour = dateTimeStr.split(" ")(1)

      eventInfo
    }
    }

    eventInfoDStream.cache()
    // 开窗
    // 	5分钟内
    val eventInfoWindowDStream: DStream[EventInfo] = eventInfoDStream.window(Seconds(300), Seconds(5))

    eventInfoWindowDStream.cache()
    // 按照mid分组
    //    	同一设备
    val eventInfoGroupDStream: DStream[(String, Iterable[EventInfo])] = eventInfoWindowDStream.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()

    //    	三次不同账号登录
    val alertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = eventInfoGroupDStream.map {
      case (mid, eventInfoItr) => {
        val uidSet = new util.HashSet[String]()
        // 优惠券涉及的商品id
        val itemSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        var clickItemFlag = false
        var alertFlag = false

        Breaks.breakable(
          for (eventInfo <- eventInfoItr) {
            // 保存发生过的行为
            eventList.add(eventInfo.evid)
            // 保存领取优惠券时的登录账号
            if (eventInfo.evid == "coupon") {
              uidSet.add(eventInfo.uid)
              itemSet.add(eventInfo.itemid)
            }
            // 判断是否有点击商品
            if (eventInfo.evid == "clickItem") {
              clickItemFlag = true
              Breaks.break()
            }

          }
        )
        // 如果同一设备不同用户登录次数大于等于3且没有点击商品，符合报警条件
        if (uidSet.size() >= 3 && clickItemFlag == false) {
          alertFlag = true
        }

        (alertFlag, CouponAlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
      }
    }

    val alertFilterDStream: DStream[(Boolean, CouponAlertInfo)] = alertInfoDStream.filter(_._1)

    //    alertFilterDStream.print()
    // 写入ES

    alertFilterDStream.foreachRDD { rdd =>
      rdd.foreachPartition { alertItr =>
        val sourceList: List[(String, CouponAlertInfo)] = alertItr.map {
          case (flag, alterInfo) =>
            // 同一设备每分钟只记录一次预警
            // 利用要保存到的数据库的 幂等性进行去重  PUT
            // ES的幂等性 是基于ID    设备+分钟级时间戳作为id
            (alterInfo.mid + "_" + alterInfo.ts / 1000 / 60, alterInfo)
        }.toList

        MyESUtil.insertESBulk(GmallConstant.ES_ALERT_INDEX, sourceList)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
