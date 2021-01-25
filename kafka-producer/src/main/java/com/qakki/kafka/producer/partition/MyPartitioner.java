package com.qakki.kafka.producer.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 分区
 *
 * @author qakki
 * @date 2021/1/25 12:46 上午
 */
public class MyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String keyStr = key + "";
        int num = Integer.parseInt(keyStr.substring(keyStr.lastIndexOf("-") + 1));
        if (num < 5) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public void close() {

    }


    @Override
    public void configure(Map<String, ?> configs) {

    }
}
