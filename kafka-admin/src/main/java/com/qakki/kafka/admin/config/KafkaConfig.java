package com.qakki.kafka.admin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * 配置
 *
 * @author qakki
 * @date 2021/1/24 7:01 下午
 */
@Configuration
public class KafkaConfig {

    public static void main(String[] args) {
        AdminClient adminClient = adminClient();
        System.out.println(adminClient);
    }

    @Bean("adminClient")
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return AdminClient.create(properties);
    }
}
