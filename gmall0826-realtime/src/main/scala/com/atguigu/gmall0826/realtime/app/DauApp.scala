package com.atguigu.gmall0826.realtime.app


import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0826.common.GmallConstant
import com.atguigu.gmall0826.realtime.bean.StartupLog
import com.atguigu.gmall0826.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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

    /* 链接redis次数太多可以进一步优化
    val filteredDstream = startUpLogDstream.filter { startuplog =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val dauKey = "dau:" + startuplog.logDate
      val flag = !jedis.sismember(dauKey, startuplog.mid)

      jedis.close()
      flag
    }*/
    //优化： 利用driver查询出完整清单，然后利用广播变量发送给各个executor
    //各个ex 利用广播变量中的清单 检查自己的数据是否需要过滤 在清单中的一律清晰掉
    /*
    错误方案，会造成driver只执行了一次  不会周期性执行
    //1 查 driver
    val jedis: Jedis = RedisUtil.getJedisClient
    val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val daukey = "dau:"+ today
    val midSet: util.Set[String] = jedis.smembers(daukey)
    jedis.close()

    //2.发
    val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

    startUpLogDstream.filter{startuplog=> // 3. ex 收 筛查
      val midSet: util.Set[String] = midBC.value
      !midSet.contains(startuplog.mid)
    }
*/
    //让driver每5秒执行一次
    val filteredDstream: DStream[StartupLog] = startUpLogDstream.transform { rdd =>
      // driver
      //1 查 driver
      println("过滤前" + rdd.count())
      val jedis: Jedis = RedisUtil.getJedisClient
      val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val daukey = "dau:" + today
      val midSet: util.Set[String] = jedis.smembers(daukey)
      jedis.close()

      //2.发
      val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)


      val filteredRDD: RDD[StartupLog] = rdd.filter { startuplog =>
        // ex 3. ex 收 筛查
        val midSet: util.Set[String] = midBC.value
        !midSet.contains(startuplog.mid)

      }
      println("过滤后" + filteredRDD.count())
      filteredRDD
      //driver
    }

  //  自检内部  分组去重 ： 0 转换成kv  1   先按mid进行分组   2 组内排序 按时间排序  3  取 top1
    val groupbyMidDstream: DStream[(String, Iterable[StartupLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val realFilteredDstream: DStream[StartupLog] = groupbyMidDstream.flatMap { case (mid, startuplogItr) =>
      val startuplogList: List[StartupLog] = startuplogItr.toList.sortWith { (startuplog1, startuplog2) =>
        startuplog1.ts < startuplog2.ts // 正序排序
      }
      val top1startupLogList: List[StartupLog] = startuplogList.take(1)
      top1startupLogList
    }





    //  利用redis保存当日访问过的用户清单


    realFilteredDstream.foreachRDD { rdd =>
      rdd.foreachPartition { startupLogItr =>
        //写入Rdis
//        val jedis = new Jedis("hadoop102",6379)
        val jedis: Jedis = RedisUtil.getJedisClient
        for (startupLog <- startupLogItr) {
          //type类型? set(不能重复)  key? dau:2020-02-07  value? mid
          val dauKey = "dau:" + startupLog.logDate

          println(startupLog)
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
