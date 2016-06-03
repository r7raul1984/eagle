package org.apache.eagle.security.hbase

import org.apache.eagle.datastream.ExecutionEnvironments
import org.apache.eagle.datastream.sparkstreaming.SparkStreamingExecutionEnvironment
import org.apache.eagle.security.hbase.parse.HbaseAuditLogKafkaDeserializer
import org.apache.eagle.security.hbase.sensitivity.HbaseResourceSensitivityDataJoinExecutor

/**
  * Created by root on 5/26/16.
  */

object TestSparkStreamingWithAlertDSLUseHBase extends App {
  val env = ExecutionEnvironments.get[SparkStreamingExecutionEnvironment](args)
  val streamName = "hbaseAuditLogStream"
  val streamExecutorId = "hbaseAuditLogExecutor"
  env.config.set("dataSourceConfig.deserializerClass", classOf[HbaseAuditLogKafkaDeserializer].getCanonicalName)
  env.fromKafka().parallelism(1).nameAs(streamName).!(Seq(streamName), streamExecutorId)
    //.flatMap(new HbaseResourceSensitivityDataJoinExecutor())
  env.execute()

}

