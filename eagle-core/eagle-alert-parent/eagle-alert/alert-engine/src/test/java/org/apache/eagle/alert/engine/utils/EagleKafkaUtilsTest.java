package org.apache.eagle.alert.engine.utils;

import kafka.common.TopicAndPartition;
import org.apache.spark.streaming.kafka.EagleKafkaUtils;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaTestUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.*;

public class EagleKafkaUtilsTest {

    private transient KafkaTestUtils kafkaTestUtils = null;
    private final String groupId = "test";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        kafkaTestUtils = new KafkaTestUtils();
        kafkaTestUtils.setup();
    }

    @After
    public void tearDown() {
        if (kafkaTestUtils != null) {
            kafkaTestUtils.teardown();
            kafkaTestUtils = null;
        }
    }

    @Test
    public void testNewTopic() throws InterruptedException {

        final String topic1 = "topic1";
        final String topic2 = "topic2";
        createTopic(topic1);
        createTopic(topic2);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaTestUtils.brokerAddress());
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("group.id", groupId);

        scala.collection.mutable.Map<String, String> mutableKafkaParam = JavaConversions
                .mapAsScalaMap(kafkaParams);
        scala.collection.immutable.Map<String, String> immutableKafkaParam = mutableKafkaParam
                .toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(
                            Tuple2<String, String> v1) {
                        return v1;
                    }
                });
        final KafkaCluster kafkaCluster = new KafkaCluster(immutableKafkaParam);
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
        Set<String> topics = new HashSet<String>();
        topics.add(topic1);
        topics.add(topic2);
        EagleKafkaUtils.fillInLatestOffsets(topics, fromOffsets, groupId, kafkaCluster, kafkaTestUtils.zkAddress());
        Assert.assertEquals(2, fromOffsets.size());
        Assert.assertEquals("{[topic2,0]=0, [topic1,0]=0}", fromOffsets.toString());
    }


    @Test
    public void testOldTopicWithOffsetAndNewTopic() throws InterruptedException {

        final String topic1 = "topic1";
        final String topic2 = "topic2";
        createTopic(topic1);
        createTopic(topic2);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaTestUtils.brokerAddress());
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("group.id", groupId);

        scala.collection.mutable.Map<String, String> mutableKafkaParam = JavaConversions
                .mapAsScalaMap(kafkaParams);
        scala.collection.immutable.Map<String, String> immutableKafkaParam = mutableKafkaParam
                .toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(
                            Tuple2<String, String> v1) {
                        return v1;
                    }
                });
        final KafkaCluster kafkaCluster = new KafkaCluster(immutableKafkaParam);


        Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
        topicAndPartitionObjectMap.put(new TopicAndPartition("topic1", 0), 12L);

        scala.collection.mutable.Map<TopicAndPartition, Object> map = JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
        scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                map.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                    public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                        return v1;
                    }
                });
        kafkaCluster.setConsumerOffsets(groupId, scalatopicAndPartitionObjectMap);
        Set<String> topics = new HashSet<String>();
        topics.add(topic1);
        topics.add(topic2);

        Map<TopicAndPartition, Long> topicAndPartitionLongMap = new HashMap<TopicAndPartition, Long>();
        topicAndPartitionLongMap.put(new TopicAndPartition("topic1", 0), 0L);

        EagleKafkaUtils.fillInLatestOffsets(topics, topicAndPartitionLongMap, groupId, kafkaCluster, kafkaTestUtils.zkAddress());
        Assert.assertEquals(2, topicAndPartitionLongMap.size());
        Assert.assertEquals(new Long(12), topicAndPartitionLongMap.get(new TopicAndPartition("topic1", 0)));
        Assert.assertEquals(new Long(0), topicAndPartitionLongMap.get(new TopicAndPartition("topic2", 0)));
    }

    @Test
    public void testInputEmptyTopics() throws InterruptedException {

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaTestUtils.brokerAddress());
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("group.id", groupId);

        scala.collection.mutable.Map<String, String> mutableKafkaParam = JavaConversions
                .mapAsScalaMap(kafkaParams);
        scala.collection.immutable.Map<String, String> immutableKafkaParam = mutableKafkaParam
                .toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(
                            Tuple2<String, String> v1) {
                        return v1;
                    }
                });
        final KafkaCluster kafkaCluster = new KafkaCluster(immutableKafkaParam);
        Set<String> topics = new HashSet<String>();
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
        thrown.expect(IllegalArgumentException.class);
        EagleKafkaUtils.fillInLatestOffsets(topics, fromOffsets, groupId, kafkaCluster, kafkaTestUtils.zkAddress());
    }


    private String[] createTopic(String topic) {
        String[] data = {topic + "-1", topic + "-2", topic + "-3"};
        kafkaTestUtils.createTopic(topic, 1);
        return data;
    }
}
