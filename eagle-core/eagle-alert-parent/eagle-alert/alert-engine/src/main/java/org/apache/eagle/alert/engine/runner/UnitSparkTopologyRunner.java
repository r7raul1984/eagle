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

import com.typesafe.config.Config;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.spark.function.*;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;
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
    private Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
    private final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
    private final AtomicReference<SpoutSpec> spoutSpecRef = new AtomicReference<SpoutSpec>();
    private final AtomicReference<Map<String, StreamDefinition>> sdsRef = new AtomicReference<Map<String, StreamDefinition>>();
    private final AtomicReference<AlertBoltSpec> alertBoltSpecRef = new AtomicReference<AlertBoltSpec>();
    private final AtomicReference<PublishSpec> publishSpecRef = new AtomicReference<PublishSpec>();
    private final AtomicReference<Set<String>> topicsRef = new AtomicReference<Set<String>>();
    private String zkServers = null;//Zookeeper server string: host1:port1[,host2:port2,...]
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
        this.groupId = "eagle" + new Random(10).nextInt();
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
        this.config = config;
        this.topologyId = config.getString("topology.name");
        this.zkServers = config.getString("zkConfig.zkQuorum");
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

        Set<String> topics = getInitTopics(config);
        EagleKafkaUtils.fillInLatestOffsets(topics, this.fromOffsets, this.groupId, this.kafkaCluster, this.zkServers);

        int windowDurations = config.getInt(WINDOW_DURATIONS);
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);


        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;


        JavaInputDStream<MessageAndMetadata<String, String>> messages = EagleKafkaUtils.createDirectStream(jssc,
                String.class, String.class, StringDecoder.class,
                StringDecoder.class, streamClass, kafkaParams,
                this.fromOffsets,fromOffsets ->fromOffsets, message -> message);
        JavaPairDStream<String, String> pairDStream = messages.transform(new Function<JavaRDD<MessageAndMetadata<String, String>>, JavaRDD<MessageAndMetadata<String, String>>>() {

            SpecMetadataServiceClientImpl client = new SpecMetadataServiceClientImpl(config);

            @Override
            public JavaRDD<MessageAndMetadata<String, String>> call(JavaRDD<MessageAndMetadata<String, String>> rdd) throws Exception {

                spoutSpecRef.set(client.getSpoutSpec());
                sdsRef.set(client.getSds());
                alertBoltSpecRef.set(client.getAlertBoltSpec());
                publishSpecRef.set(client.getPublishSpec());
                topicsRef.set(getTopics(client));

                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
            }

            private Set<String> getTopics(SpecMetadataServiceClientImpl client) {
                return client.getSpoutSpec().getKafka2TupleMetadataMap().keySet();
            }
        }).mapToPair(km -> new Tuple2<>(km.topic(), km.message()));
        pairDStream.window(Durations.seconds(windowDurations), Durations.seconds(windowDurations))
                .flatMapToPair(new CorrelationSpoutSparkFunction(numOfRouter, spoutSpecRef, sdsRef))
                .transformToPair(new ChangePartitionTo(numOfRouter))
                .mapPartitionsToPair(new StreamRouteBoltFunction(config, "streamBolt"))
                .transformToPair(new ChangePartitionTo(numOfAlertBolts))
                .mapPartitionsToPair(new AlertBoltFunction(alertBoltNamePrefix, sdsRef, alertBoltSpecRef, numOfAlertBolts))
                .repartition(numOfPublishTasks)
                .foreachRDD(new Publisher(publishSpecRef, sdsRef, alertPublishBoltName, kafkaCluster, groupId, offsetRanges));
    }

    private Set<String> getInitTopics(Config config) {
        SpecMetadataServiceClientImpl client = new SpecMetadataServiceClientImpl(config);
        return client.getSpoutSpec().getKafka2TupleMetadataMap().keySet();
    }

}
