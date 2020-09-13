package sparkstreaming

import java.sql.{Connection, ResultSet}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{DataSourceUtil, QueryCallback, SqlProxy}

import scala.collection.mutable

/**
 * @Author: Zhangbao
 * @Date: 11:42 2020/9/13
 * @Description:
 *  注册需求：消费者和业务处理流程
 */
object RegisterStreaming {
  //指定消费者组：自动创建
  private val groupId = "register_group_test"

  def main(args: Array[String]): Unit = {
    //指定hadoop管理用户为：root
    System.setProperty("HADOOP_USER_NAME", "root")

    //指定spark配置和创建程序入口
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
      //控制消费速度的参数，意思是每个分区上每秒钟消费的条数--只是最大消费速率
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      //开启背压机制，自适应调节kafka接收和消费速率--（消费速率，设置的最大消费速率）二者最小值
      //.set("spark.streaming.backpressure.enabled", "true") //默认开启
      //优雅停止sparkStreaming
      //.set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    //sparkStreaming程序入口，第二个参数为批次时间，也是延迟时间
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //sparkContext上下文连接集群初始化配置
    val sc: SparkContext = ssc.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    //用数组存放topic，意思就是我这里可以监控多个topic
    val topics: Array[String] = Array("register_topic")
    //kafka消费者配置的Map，注意Map里面的泛型必须是[String, Object]
    val kafkaMap: Map[String, Object] = Map[String, Object](
      //kafka监控地址
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      //kafka反序列化
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //消费者组
      "group.id" -> groupId,
      //找不到偏移量的时候，自动重置偏移量：
      /*
      *（1）sparkstreaming第一次启动，不丢数
      */
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //sparkStreaming对有状态的数据操作，需要设定检查点目录，让后将状态保存到检查点中
    /*此处不合理：
    检查点会产生大量小文件到hdfs上
     */
    ssc.checkpoint("/user/atguigu/sparkstreaming/checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy: SqlProxy = new SqlProxy()
    val offsetMap: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()
    val conn: Connection = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(conn,
        """
          |select *
          |from `offset_manager`
          |where groupid = ?""".stripMargin,
        Array(groupId),
        new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val topicPartition: TopicPartition = new TopicPartition(rs.getString(2), rs.getInt(3))
              val offset: Long = rs.getLong(4)
              //获取偏移量
              offsetMap.put(topicPartition, offset)
            }
            rs.close() //关闭游标，结果集关闭
          }
        })
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(conn)
    }
    //设置kafka消费的参数，判断本地是否有偏移量，防止空指针异常
    val inputStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      //找不到偏移量,重新消费
      //第一个参数是存放一个StreamingContext
      //第二个参数是消费数据平衡策略，这里用的是均匀消费
      //第三个参数是具体要监控的对象（里面包含topic，kafkaMap，偏移量等配置）
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      //如果有偏移量从偏移量开始消费
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    //对输入数据流进行结构转换，和简单的ETL
    val dataStream: DStream[(String, Long)] = inputStream.filter(item => {
      //过滤掉数据不合规的数据
      item.value().split("\t").length == 3
    }).mapPartitions(partitions => {
      /* MapPartitions的优点：
      如果是普通的map，比如一个partition中有1万条数据。ok，那么你的function要执行和计算1万次。
      使用MapPartitions操作之后，一个task仅仅会执行一次function，function一次接收所有
      的partition数据。只要执行一次就可以了，性能比较高。如果在map过程中需要频繁创建额外
      的对象(例如将rdd中的数据通过jdbc写入数据库,map需要为每个元素创建一个链接而mapPartition
      为每个partition创建一个链接),则mapPartitions效率比map高的多。

      SparkSql或DataFrame默认会对程序进行mapPartition的优化。

      MapPartitions的缺点：
      如果是普通的map操作，一次function的执行就处理一条数据；那么如果内存不够用的情况下， 比如处理了1千条数据了，那么这个时候内存不够了，那么就可以将已经处理完的1千条数据从内存里面垃圾回收掉，或者用其他方法，腾出空间来吧。
      所以说普通的map操作通常不会导致内存的OOM异常。
      但是MapPartitions操作，对于大量数据来说，比如甚至一个partition，100万数据，
      一次传入一个function以后，那么可能一下子内存不够，但是又没有办法去腾出内存空间来，可能就OOM，内存溢出。*/
      partitions.map(item => {
        val line: String = item.value()
        val arr: Array[String] = line.split("\t")
        //模式匹配，对AppName进行解析转化
        val appName: String = arr(1) match {
          case "1" => "PC"
          case "2" => "APP"
          case _ => "Other"
        }
        (appName, 1L)
      })
    })
    //缓存，用作RDD持久化
    dataStream.cache()

    //---------------------------------------------------------------------------------
    // 以上截至的数据流：(PC,1),(PC,1),(APP,1),(Other,1),(APP,1),(Other,1),(PC,1),(APP,1)
    //---------------------------------------------------------------------------------

    //"===========业务需求：每6s间隔1分钟内的注册数据================="
    //滑动窗口，注意窗口大小和滑动步长必须是批次时间的整数倍
    // 第一个参数是具体要做什么操作
    // 第二个参数是窗口大小
    // 第三个参数是滑动步长

    //-----输出当前窗口的各appName的注册人数
    val currResultStream: DStream[(String, Long)] = dataStream.reduceByKeyAndWindow(
      (x: Long, y: Long) => x + y,
      Seconds(60),
      Seconds(6)
    )
    currResultStream.print()

    //-----输出历史各appName的注册总人数
    //状态计算函数，updateStateByKey的必要参数
    val updateFunc: (Seq[Long], Option[Long]) => Some[Long] = (value: Seq[Long], state: Option[Long]) => {
      val currentCount: Long = value.sum //本批次求和
      val previousCount: Long = state.getOrElse(0) //历史数据
      Some(currentCount + previousCount)
    }
    //updateStateByKey算子会去跟历史状态数据做一个计算，所以要提前设置一个历史状态保存路径，即检查点Checkpoint
    dataStream.updateStateByKey(updateFunc).print()

    //数据倾斜解决方式如下：两阶段聚合

    //    val dsStream = stream.filter(item => item.value().split("\t").length == 3)
    //      .mapPartitions(partitions =>
    //        partitions.map(item => {
    //          val rand = new Random()
    //          val line = item.value()
    //          val arr = line.split("\t")
    //          val app_id = arr(1)
    //          //将key打散（该方式欠妥），应该找出数据倾斜严重的key，将这部分key进行打散
    //          (rand.nextInt(3) + "_" + app_id, 1L) //(0_1,1L),(0_2,1L),(0_3,1L),(1_1,1L),(1_2,1L),(1_2,1L),(2_1,1L),(2_2,1L),(2_3,1L)
    //        }))
    //    val result = dsStream.reduceByKey(_ + _)
    //    result.map(item => {
    //      val appid = item._1.split("_")(1)
    //      (appid, item._2)
    //    }).reduceByKey(_ + _).print()

    //*处理完业务逻辑后，要对kafka消费到的inputStream，进行手动提交offset到数据库
    inputStream.foreachRDD(rdd =>{
      //数据库操作代理工具
      val sqlProxy: SqlProxy = new SqlProxy
      //获取数据库连接
      val conn: Connection = DataSourceUtil.getConnection
        try {
          //对RDD中的每个分区获取该分区当次消费的元数据信息的数组
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for (offsetRange <- offsetRanges) {
            sqlProxy.executeUpdate(conn,
              """
                |replace into `offset_manager` (groupid,topic,`partition`,untilOffset)
                |values(?,?,?,?)""".stripMargin,
              Array(groupId,offsetRange.topic,offsetRange.partition,offsetRange.untilOffset))
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(conn)
        }
    })
    /*
    拓展：
    //1.=======执行错误：========================
      dstream.foreachRDD { rdd =>
        val connection = createNewConnection()  // executed at the driver
        rdd.foreach { record =>
          connection.send(record) // executed at the worker
        }
      }
    //2.=======正确执行：数据库连接数过多==========
      dstream.foreachRDD { rdd =>
        rdd.foreach { record =>
          val connection = createNewConnection()
          connection.send(record)
          connection.close()
        }
      }
    //3.=======正确执行：优化====================
      dstream.foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val connection = createNewConnection()
          partitionOfRecords.foreach(record => connection.send(record))
          connection.close()
        }
      }
     */


    //StreamingContext程序执行，挂起
    ssc.start()
    ssc.awaitTermination()
  }
}
