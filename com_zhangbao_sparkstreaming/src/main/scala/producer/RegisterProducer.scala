package producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Zhangbao
 * @Date: 9:59 2020/9/13
 * @Description:
 *  kakfa生产者
 *  包含：调优
 *       精准一次性
 */

/**
 * 注册需求kafka生产者
 */
object RegisterProducer {
  def main(args: Array[String]): Unit = {
    //集群参数配置
    val sparkConf: SparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
    //spark程序入口：连接spark集群，初始化spark集群
    /*SparkContext是spark功能的主要入口。其代表与spark集群的连接，能够用来在集群上创建RDD、累加器、广播变量。
     每个JVM里只能存在一个处于激活状态的SparkContext，在创建新的SparkContext之前必须调用stop()来关闭之前
     的SparkContext。*/
    val sc: SparkContext = new SparkContext(sparkConf)
    //设置10个分区为了和kafka中的分区数对应1:1的关系，这样发送速度更快
    //sc.textFile("/user/atguigu/kafka/register.log",10) //hdfs的路径
    sc.textFile("file://"+this.getClass.getResource("/register.log").getPath, 10)
      //使用foreachPartition减少与数据库的连接数，导致连接超时
      .foreachPartition(partition => {
        //每个分区创建一个kafka连接
        val props: Properties = new Properties()
        //kafka服务连接节点
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        //ack的设置（0 ，1 ，-1或all）根据生产需求设置acks
        props.put("acks", "1")
        //producer批量发送的基本单位，默认是16384Bytes
        props.put("batch.size", "16384")
        //linger.ms是sender线程在检查batch是否ready时候，判断有没有过期的参数，默认大小是0ms
        props.put("linger.ms", "10") //满足batch.size和ling.ms之一，producer便开始发送消息
        /*Kafka的客户端发送数据到服务器，一般都是要经过缓冲的，也就是说，
        你通过KafkaProducer发送出去的消息都是先进入到客户端本地的内存缓冲里，
        然后把很多消息收集成一个一个的Batch，再发送到Broker上去的。*/
        props.put("buffer.memory", "33554432")
        //**一定要指定序列化，否则报错--一般都是字符串序列化
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        //根据配置创建生产者对象
        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
        //遍历当前分区中的数据
        partition.foreach(item => {
          //每条消息封装成一个生产者记录（包含要写入的主题，还有每条消息内容）
          /*生产者分区分配原则：ProducerRecord根据参数不同，回想成不同的分区分配原则
          * （1）指定partition
          * （2）没指定partition但有key
          * （3）没指定partition也没有key--随机生成一个值对partition数取余得到分区值，轮询算法
          */
          val msgRecord: ProducerRecord[String, String] = new ProducerRecord[String, String]("register_topic", item)
          //异步发送消息（默认）
          /*
          * （1）不带回调函数
          * （2）带回调函数：回调函数会在producer收到ack时调用，用来显示的说明消息发送成功还是失败
          * 注意：无论带不带回调函数，消息发送失败会自动重试，不需要我们在回调函数中手动重试。
          * */

          /*
          * （1）异步发送消息
          *     Kafka的Producer发送消息采用的是异步发送的方式。在消息发送的过程中，涉及到了两个线程
          *     ——main线程和Sender线程，以及一个线程共享变量——RecordAccumulator。main线程将消息发
          *     送给RecordAccumulator，Sender线程不断从RecordAccumulator中拉取消息发送到Kafka broker。
          * （2）同步发送消息
          *     同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回ack。
          *     由于send方法返回的是一个Future对象，根据Futrue对象的特点，我们也可以实现同步发送的
          *     效果，只需在调用Future对象的get方发即可。
          * */
          producer.send(msgRecord)
          //producer.send(msgRecord).get() //同步发送
        })
        //刷新，关闭生产者
        producer.flush()
        producer.close()
      })



  }


}
