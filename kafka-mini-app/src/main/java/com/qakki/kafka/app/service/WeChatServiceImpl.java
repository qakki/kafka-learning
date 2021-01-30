package com.qakki.kafka.app.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qakki.kafka.app.conf.WechatTemplateProperties;
import com.qakki.kafka.app.utils.FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * WeChatServiceImpl
 *
 * @author qakki
 * @date 2021/1/30 4:42 下午
 */
@Slf4j
@Service
public class WeChatServiceImpl implements WeChatService {
    @Autowired
    private WechatTemplateProperties properties;

    @Autowired
    private Producer producer;

    @Override
    public WechatTemplateProperties.WechatTemplate getWechatTemplate() {
        List<WechatTemplateProperties.WechatTemplate> templates = properties.getTemplates();
        Optional<WechatTemplateProperties.WechatTemplate> wechatTemplate
                = templates.stream().filter(WechatTemplateProperties.WechatTemplate::isActive).findFirst();
        return wechatTemplate.orElse(null);
    }

    @Override
    public void templateReported(JSONObject reportInfo) {
        // kafka producer将数据推送至Kafka Topic
        log.info("templateReported : [{}]", reportInfo);
        String topicName = "qakki_info_topic";
        // 发送Kafka数据
        String templateId = reportInfo.getString("templateId");
        JSONArray reportData = reportInfo.getJSONArray("result");

        // 如果templateid相同，后续在统计分析时，可以考虑将相同的id的内容放入同一个partition，便于分析使用
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topicName, templateId, reportData);
        try {
            producer.send(record);
        } catch (Exception e) {
            log.error("发送失败");
        }
    }

    @Override
    public JSONObject templateStatistics(String templateId) {
        // 判断数据结果获取类型
        if (properties.getTemplateResultType() == 0) {
            Optional<JSONObject> optional = FileUtils.readFile2JsonObject(properties.getTemplateResultFilePath());
            if (optional.isPresent()) {
                return optional.get();
            }
        }
        return null;
    }
}
