package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.utils.JSONUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class JsonFileOffsetReader implements IOffsetReader{
    private String file;
    private BufferedReader bufferedReader;

    public JsonFileOffsetReader(String file) {
        this.file = file;
    }

    @Override
    public KafkaTopicOffset readOffset(String group, String topic) throws Exception {
        bufferedReader = new BufferedReader(new FileReader(file));
        String offsetStr = bufferedReader.readLine();
        if (offsetStr != null) {
            return JSONUtils.jsonToPojo(offsetStr, KafkaTopicOffset.class);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (bufferedReader != null) {
            bufferedReader.close();
        }
    }
}
