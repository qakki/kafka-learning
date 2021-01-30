package com.qakki.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * consumer测试
 *
 * @author qakki
 * @date 2021/1/30 6:23 下午
 */
public class ConsumerTest {
    private static final String TOPIC_NAME = "qakki_info_topic";

    public static void main(String[] args) {
        // helloWorld();
        commitOffset();
    }

    /**
     * demo
     */
    private static void helloWorld() {
        KafkaConsumer<String, String> consumer = getKafkaConsumer();
        // 消费订阅哪一个Topic或者几个Topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("拉取消息patition = %d , offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 手动提交offset
     */
    private static void commitOffset() {
        KafkaConsumer<String, String> consumer = getKafkaConsumer();
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        // 消费订阅某个Topic的某个分区
        consumer.assign(Arrays.asList(p0, p1));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("订阅patition = %d , offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                // 提交offset
                consumer.commitSync(offset);
                System.out.println("=============partition - " + partition + " end================");
            }
        }
    }

    private static KafkaConsumer<String, String> getKafkaConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("group.id", "test");
        // 是否自动提交
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props);
    }

    private static void controlPause() {
        KafkaConsumer<String, String> consumer = getKafkaConsumer();

        // jiangzh-topic - 0,1两个partition
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        // 消费订阅某个Topic的某个分区
        consumer.assign(Arrays.asList(p0, p1));
        long totalNum = 40;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                long num = 0;
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                    /*
                        1、接收到record信息以后，去令牌桶中拿取令牌
                        2、如果获取到令牌，则继续业务处理
                        3、如果获取不到令牌， 则pause等待令牌
                        4、当令牌桶中的令牌足够， 则将consumer置为resume状态
                     */
                    num++;
                    if (record.partition() == 0) {
                        if (num >= totalNum) {
                            consumer.pause(Arrays.asList(p0));
                        }
                    }

                    if (record.partition() == 1) {
                        if (num == 40) {
                            consumer.resume(Arrays.asList(p0));
                        }
                    }
                }

                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                // 提交offset
                consumer.commitSync(offset);
                System.out.println("=============partition - " + partition + " end================");
            }
        }
    }


}
