package com.stt.spark.dw.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stt.spark.dw.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class LogJsonController {
    private static final Logger logger = LoggerFactory.getLogger(LogJsonController.class) ;
//
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/log")
    public void shipLog(@RequestParam("log") String log){

        JSONObject logJsonObj = JSON.parseObject(log);

        int randomInt = new Random().nextInt(3600 * 1000 * 5);
        logJsonObj.put("ts",System.currentTimeMillis()+randomInt);

        if("startup".equals(logJsonObj.getString("type"))){
            kafkaTemplate.send(GmallConstant.TOPIC_STARTUP,logJsonObj.toJSONString());
        }else {
            kafkaTemplate.send(GmallConstant.TOPIC_EVENT,logJsonObj.toJSONString());
        }
        String logNew = logJsonObj.toJSONString();
        logger.info(logNew);
    }

}
 
