package com.bigdatalighter.kafka;

import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.offset.writer.IOffsetWriter;
import com.bigdatalighter.kafka.offset.writer.JsonFileOffsetWriter;
import com.bigdatalighter.kafka.offset.writer.OffsetWriterFactory;
import com.bigdatalighter.kafka.offset.writer.OffsetWriterParams;
import com.bigdatalighter.kafka.utils.JSONUtils;
import junit.framework.TestCase;

import java.io.File;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetWriterTests extends TestCase {
    private String zk = "spark2:2181";
    private KafkaTopicOffset offset = JSONUtils.jsonToPojo("{\"group\":\"myGroup\",\"name\":\"test\",\"offsets\":{\"0\":{\"partition\":0,\"offset\":8}}}", KafkaTopicOffset.class);

    public void testJsonFileOffsetWriter() throws Exception {
        String fileName = "./offset.json";
        OffsetWriterParams params = new OffsetWriterParams();
        params.setFi(fileName);
        params.setType(OffsetWriterFactory.JSON_FILE);
        testOffsetWriter(OffsetWriterFactory.create(params));
        //check
        File file = new File(fileName);
        assertTrue(file.exists());
        file.delete();
    }

    public void testZkOffsetWriter() throws Exception{
        testZkOffsetWriter(OffsetWriterFactory.CAMUS, "camus");
        testZkOffsetWriter(OffsetWriterFactory.CONSUMER08, "kafka");
        testZkOffsetWriter(OffsetWriterFactory.KAFKA_SPOUT, "stormetl");
    }

    public void testZkOffsetWriter(String type, String namespace) throws Exception {
        testOffsetWriter(getZkWriter(type, namespace));
    }

    public void testOffsetWriter(IOffsetWriter offsetWriter) throws Exception{
        offsetWriter.writeOffset(offset);
        offsetWriter.close();
    }

    public IOffsetWriter getZkWriter(String type, String namespace) {
        OffsetWriterParams params = new OffsetWriterParams();
        params.setType(type);
        params.setZookeeper(zk);
        params.setZkPath(namespace);
        return OffsetWriterFactory.create(params);
    }


}
