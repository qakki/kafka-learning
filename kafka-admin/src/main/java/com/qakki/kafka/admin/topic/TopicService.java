package com.qakki.kafka.admin.topic;

import com.google.common.collect.ImmutableMap;
import com.qakki.kafka.admin.config.KafkaConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * topic
 *
 * @author qakki
 * @date 2021/1/24 7:08 下午
 */
@Service
public class TopicService {

    @Autowired
    private AdminClient adminClient;

    public static void createTopic() {
        AdminClient adminClient = KafkaConfig.adminClient();
        short rs = 1;
        NewTopic topic = new NewTopic("qakki_info_topic", 2, rs);
        CreateTopicsResult res = adminClient.createTopics(Collections.singletonList(topic));
        System.out.println(res);
    }

    public static void main(String[] args) throws Exception {
        // createTopic();
        // deleteTopic("qakki_info_topic");
        System.out.println(getTopics());
        describeTopics(Collections.singletonList("test_topic"));
        describeConfig();
        increasePartition(2);
    }

    public static Set<String> getTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaConfig.adminClient();
        ListTopicsResult result = adminClient.listTopics();
        return result.names().get();
    }

    public static void deleteTopic(String topic) {
        AdminClient adminClient = KafkaConfig.adminClient();
        adminClient.deleteTopics(Collections.singletonList(topic));
    }

    public static void describeTopics(Collection<String> topics) throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaConfig.adminClient();
        DescribeTopicsResult result = adminClient.describeTopics(topics);
        System.out.println(result.all().get());
    }

    public static void describeConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaConfig.adminClient();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "test_topic");
        DescribeConfigsResult result = adminClient.describeConfigs(Collections.singletonList(configResource));
        System.out.println(result.all().get());
    }

    public static void increasePartition(int partitions) throws ExecutionException, InterruptedException {
        AdminClient adminClient = KafkaConfig.adminClient();
        CreatePartitionsResult result =
                adminClient.createPartitions(ImmutableMap.of("qakki_info_topic", NewPartitions.increaseTo(partitions)));
        System.out.println(result.all().get());
    }

}
