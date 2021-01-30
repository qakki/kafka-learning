package com.qakki.kafka.app.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.qakki.kafka.app.common.BaseResponseVO;
import com.qakki.kafka.app.conf.WechatTemplateProperties;
import com.qakki.kafka.app.service.WeChatService;
import com.qakki.kafka.app.utils.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * controller
 *
 * @author qakki
 * @date 2021/1/30 4:37 下午
 */
@Controller
@RequestMapping("wc")
public class WeChatController {

    @Autowired
    private WeChatService weChatService;

    @RequestMapping(value = "/template", method = RequestMethod.GET)
    @ResponseBody
    public BaseResponseVO<Map<String, Object>> getTemplate() {
        WechatTemplateProperties.WechatTemplate wechatTemplate = weChatService.getWechatTemplate();
        Map<String, Object> result = Maps.newHashMap();
        result.put("templateId", wechatTemplate.getTemplateId());
        result.put("template", FileUtils.readFile2JsonArray(wechatTemplate.getTemplateFilePath()));
        return BaseResponseVO.success(result);
    }

    @RequestMapping(value = "/template/result", method = RequestMethod.GET)
    @ResponseBody
    public BaseResponseVO<JSONObject> templateStatistics(
            @RequestParam(value = "templateId", required = false) String templateId) {
        JSONObject statistics = weChatService.templateStatistics(templateId);
        return BaseResponseVO.success(statistics);
    }

    @RequestMapping(value = "/template/report", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVO<Void> dataReported(
            @RequestBody String reportData) {
        weChatService.templateReported(JSON.parseObject(reportData));
        return BaseResponseVO.success();
    }
}
