package com.bigdatalighter.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class JSONUtils {

    //    static GsonBuilder gsonBuilder = new GsonBuilder();
    static ObjectMapper objectMapper = new ObjectMapper();

    static {
//        gsonBuilder.disableHtmlEscaping();
    }

    public static String toJsonString(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <T> T jsonToPojo(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }

}
