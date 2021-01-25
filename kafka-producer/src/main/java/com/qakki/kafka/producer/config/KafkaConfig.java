package com.qakki.kafka.producer.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 配置
 *
 * @author qakki
 * @date 2021/1/24 11:55 下午
 */
public class KafkaConfig {

    private static final String TOPIC_NAME = "test_topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        producerSend();
    }

    public static void producerSend() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16348");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.qakki.kafka.producer.partition.MyPartitioner");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "val-" + i);
            // 阻塞 等待返回
//            Future<RecordMetadata> future = producer.send(record);
//            RecordMetadata recordMetadata = future.get();
//            System.out.println("partition=" + recordMetadata.partition() + " offset=" + recordMetadata.offset());

            // 异步回调
            producer.send(record, (recordMetadata, e) -> System.out.println("partition=" + recordMetadata.partition() + " offset=" + recordMetadata.offset()));

        }
        producer.close();
    }
}
