package org.apache.eagle.security.hbase

import java.util.{Date, Properties}

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.util.Random
;

/**
  * Created by root on 5/26/16.
  */
object KafkaProducer {

  def main(args: Array[String]) {

    val rnd = new Random()

    val props = new Properties()
    props.put("metadata.broker.list", "wangyuming01:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)

    val producer = new Producer[String, String](config)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")

    while (true) {
      val runtime = new Date()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = format.format(runtime) + " TRACE SecurityLogger.org.apache.hadoop.hbase.security.access.AccessController: Access allowed for user root; reason: Table permission granted; remote address: /" + ip + "; request: put; context: (user=root, scope=alertStream, family=f:dataSource|f:site|f:streamName, action=WRITE)"
      val data = new KeyedMessage[String, String]("sandbox_hbase_audit_log", msg)
      producer.send(data)
      Thread.sleep(3000)
    }

    producer.close

  }

}
