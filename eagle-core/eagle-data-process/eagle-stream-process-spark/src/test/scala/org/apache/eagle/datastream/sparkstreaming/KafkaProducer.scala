package org.apache.eagle.datastream.sparkstreaming

import java.util.{Date, Properties}

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

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

    while (true) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = "{\"host\": \"/192.168.6.227\",\"source\": \"/192.168.6." + rnd.nextInt(255) + "\",\"user\": \"jaspa\",\"timestamp\": " + runtime + ",\"category\": \"QUERY\",\"type\": \"CQL_SELECT\",\"ks\": \"dg_keyspace\",\"cf\": \"customer_details\",\"operation\": \"CQL_SELECT\",\"masked_columns\": [\"bank\", \"ccno\", \"email\", \"ip\", \"name\", \"sal\", \"ssn \", \"tel\", \"url\"], \"other_columns\": [\"id\", \"npi\"]}"
      val data = new KeyedMessage[String, String]("noise", msg)
      producer.send(data)
      Thread.sleep(3000)
    }

    producer.close

  }

}
