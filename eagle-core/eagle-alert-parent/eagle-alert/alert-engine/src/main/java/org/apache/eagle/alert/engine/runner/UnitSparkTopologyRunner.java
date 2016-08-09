/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.runner;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.spark.function.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class UnitSparkTopologyRunner implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner.class);
    //kafka config
    private KafkaCluster kafkaCluster = null;
    private Map<String, String> kafkaParams = new HashMap<String, String>();
    private final static String CONSUMER_KAFKA_TOPIC = "topology.topics";
    private Set<String> topics = new HashSet<String>();
    private java.util.Map<kafka.common.TopicAndPartition, Long> fromOffsets = new java.util.HashMap<kafka.common.TopicAndPartition, Long>();
    private final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

    //spark config
    private final static String WINDOW_SECOND = "topology.window";
    private final static int DEFAULT_WINDOW_SECOND = 2;
    private final static String SPARK_EXECUTOR_CORES = "topology.core";
    private final static String SPARK_EXECUTOR_MEMORY = "topology.memory";
    private final static String alertBoltNamePrefix = "alertBolt";
    private final static String alertPublishBoltName = "alertPublishBolt";
    private final static String SPARK_EXECUTOR_INSTANCES = "topology.spark.executor.num"; //no need to set if you open spark.dynamicAllocation.enabled  see https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation
    private final static String LOCAL_MODE = "topology.localMode";
    private final static String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    private final static String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    private final static String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";

    private final static String WINDOW_DURATIONS = "topology.windowDurations";
    private final static String CHECKPOINT_DIRECTORY = "topology.checkpointDirectory";
    private final static String TOPOLOGY_MASTER = "topology.master";


    //  private final IMetadataChangeNotifyService metadataChangeNotifyService;
    private String topologyId;
    private String groupId;
    private SparkConf sparkConf;
    private final Config config;
    private long window;


    public UnitSparkTopologyRunner(IMetadataChangeNotifyService metadataChangeNotifyService, Config config) throws InterruptedException {

        String inputBroker = config.getString("spout.kafkaBrokerZkQuorum");
        kafkaParams.put("metadata.broker.list", inputBroker);
        this.groupId = "eagle";
        kafkaParams.put("group.id", this.groupId);
        kafkaParams.put("auto.offset.reset", "largest");
        // Newer version of metadata.broker.list:
        kafkaParams.put("bootstrap.servers", inputBroker);

        scala.collection.mutable.Map<String, String> mutableKafkaParam = JavaConversions
                .mapAsScalaMap(kafkaParams);
        scala.collection.immutable.Map<String, String> immutableKafkaParam = mutableKafkaParam
                .toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(
                            Tuple2<String, String> v1) {
                        return v1;
                    }
                });
        this.kafkaCluster = new KafkaCluster(immutableKafkaParam);
        String topic = config.getString(CONSUMER_KAFKA_TOPIC);
        this.topics.add("oozie");


        this.config = config;
        this.topologyId = config.getString("topology.name");

        this.window = config.hasPath(WINDOW_SECOND) ? config.getLong(WINDOW_SECOND) : DEFAULT_WINDOW_SECOND;
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(topologyId);
        boolean localMode = config.getBoolean(LOCAL_MODE);
        if (localMode) {
            LOG.info("Submitting as local mode");
            sparkConf.setMaster("local[*]");
        } else {
            sparkConf.setMaster(config.getString(TOPOLOGY_MASTER));
        }
        String sparkExecutorCores = config.getString(SPARK_EXECUTOR_CORES);
        String sparkExecutorMemory = config.getString(SPARK_EXECUTOR_MEMORY);
        // String checkpointDir = config.getString(CHECKPOINT_DIRECTORY);
        sparkConf.set("spark.executor.cores", sparkExecutorCores);
        sparkConf.set("spark.executor.memory", sparkExecutorMemory);

        this.sparkConf = sparkConf;
        // this.jssc.checkpoint(checkpointDir);
        /*this.metadataChangeNotifyService = metadataChangeNotifyService;
        this.metadataChangeNotifyService.registerListener(this);
        this.metadataChangeNotifyService.init(config, MetadataType.ALL);*/

    }

    public void run() throws InterruptedException {
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(window));
        buildTopology(jssc, config);
        jssc.start();
        jssc.awaitTermination();
    }

    private void buildTopology(JavaStreamingContext jssc, Config config) {


        fillInLatestOffsets();

        int windowDurations = config.getInt(WINDOW_DURATIONS);
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);


        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;
        JavaInputDStream<MessageAndMetadata<String, String>> messages = KafkaUtils.createDirectStream(jssc,
                String.class, String.class, StringDecoder.class,
                StringDecoder.class, streamClass, kafkaParams,
                this.fromOffsets, message -> message);
        JavaPairDStream<String, String> pairDStream = messages.transform(new Function<JavaRDD<MessageAndMetadata<String, String>>, JavaRDD<MessageAndMetadata<String, String>>>() {
            @Override
            public JavaRDD<MessageAndMetadata<String, String>> call(JavaRDD<MessageAndMetadata<String, String>> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
            }
        }).mapToPair(km -> new Tuple2<>(km.topic(), km.message()));
        pairDStream.window(Durations.seconds(windowDurations), Durations.seconds(windowDurations))
                .flatMapToPair(new CorrelationSpoutSparkFunction(numOfRouter, config))
                .transformToPair(new ChangePartitionTo(numOfRouter))
                .mapPartitionsToPair(new StreamRouteBoltFunction(config, "streamBolt"))
                .transformToPair(new ChangePartitionTo(numOfAlertBolts))
                .mapPartitionsToPair(new AlertBoltFunction(alertBoltNamePrefix, config, numOfAlertBolts))
                .repartition(numOfPublishTasks).foreachRDD(new Publisher(config, alertPublishBoltName, kafkaCluster, groupId, offsetRanges));
    }

    private void fillInLatestOffsets() {
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions
                .asScalaSet(this.topics);
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics
                .toSet();
        scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet = kafkaCluster
                .getPartitions(immutableTopics).right().get();
        if (kafkaCluster.getConsumerOffsets(groupId,
                scalaTopicAndPartitionSet).isLeft()) {
            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions
                    .setAsJavaSet(scalaTopicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
                this.fromOffsets.put(topicAndPartition, 0L);
            }
        } else {
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster
                    .getConsumerOffsets(groupId,
                            scalaTopicAndPartitionSet).right().get();

            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions
                    .mapAsJavaMap(consumerOffsetsTemp);
            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions
                    .setAsJavaSet(scalaTopicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                this.fromOffsets.put(topicAndPartition, offset);
            }
        }
    }

}
