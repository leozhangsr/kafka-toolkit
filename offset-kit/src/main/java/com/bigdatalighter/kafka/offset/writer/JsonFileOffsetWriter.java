package com.bigdatalighter.kafka.offset.writer;

import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.offset.reader.IOffsetReader;
import com.bigdatalighter.kafka.utils.JSONUtils;

import java.io.*;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class JsonFileOffsetWriter implements IOffsetWriter {
    private String file;
    private FileWriter fileWriter;

    public JsonFileOffsetWriter(String file) {
        this.file = file;
    }

    @Override
    public void writeOffset(KafkaTopicOffset offset) throws Exception {
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write(JSONUtils.toJsonString(offset));
        fileWriter.flush();
    }



    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            fileWriter.close();
        }
    }
}
