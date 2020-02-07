package com.atguigu.gmall0826.realtime.app

import java.sql.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0826.common.GmallConstant
import com.atguigu.gmall0826.realtime.bean.StartupLog
import com.atguigu.gmall0826.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DateFormat
import redis.clients.jedis.Jedis


object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Dau_app").setMaster("local[*]")


    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    //    recordDstream.map(_.value()).print()
    // 1 进行一个格式转换 补充时间字段
    val startUpLogDstream: DStream[StartupLog] = recordDstream.map { record =>
      val jsonString: String = record.value()

      val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val datetimeString: String = dateFormat.format(new Date(startupLog.ts))

      val datetimeArr: Array[String] = datetimeString.split(" ")


      startupLog.logDate = datetimeArr(0)
      startupLog.logHour = datetimeArr(1)

      startupLog

    }


    //2  去重   保留每个mid当日的第一条   其他的启动日志过滤掉

    //  然后再利用清单进行过滤筛选 把清单中已有的用户的新日志过滤掉


    //  利用redis保存当日访问过的用户清单


    startUpLogDstream.foreachRDD { rdd =>
      rdd.foreachPartition { startupLogItr =>
        //写入Rdis
        val jedis = new Jedis("hadoop102",6379)
        for (startupLog <- startupLogItr) {
          //type类型? set(不能重复)  key? dau:2020-02-07  value? mid
          val dauKey = "dau:" + startupLog.logDate
          jedis.sadd(dauKey, startupLog.mid)
          jedis.expire(dauKey, 60 * 60 * 24) //保留24小时,失效
        }
        jedis.close()

       }

      }


      //      rdd.foreach { startuplog =>
      //        //写入Rdis
      //        val jedis = new Jedis("hadoop102", 6379)
      //        //type类型? set(不能重复)  key? dau:2020-02-07  value? mid
      //        val dauKey = "dau:" + startuplog.logDate
      //        jedis.sadd(dauKey, startuplog.mid)
      //        jedis.expire(dauKey, 60 * 60 * 24) //保留24小时,失效
      //        //        jedis.expireAt(dauKey,)//定点失效
      //
      //        jedis.close()
      //
      //      }


    ssc.start()
    ssc.awaitTermination()

  }
}
