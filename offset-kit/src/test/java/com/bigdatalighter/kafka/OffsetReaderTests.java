package com.bigdatalighter.kafka;

import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.offset.reader.IOffsetReader;
import com.bigdatalighter.kafka.offset.reader.OffsetReaderFactory;
import com.bigdatalighter.kafka.offset.reader.OffsetReaderParams;
import com.bigdatalighter.kafka.utils.CuratorManager;
import junit.framework.TestCase;
import org.junit.After;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetReaderTests extends TestCase {
    private String zk = "spark2:2181";
    private String broker = "spark2:9092";
    private String topic = "test";
    private String group = "myGroup";
    private String fi = "./offset.json";

    public void testJsonFileOffsetReader() throws Exception {
        OffsetReaderParams params = new OffsetReaderParams();
        params.setType(OffsetReaderFactory.JSON_FILE);
        params.setFi(fi);
        testOffsetReader(params);
    }

    public void testKafkaLogOffsetReader() throws Exception {
        testOffsetReader(getParamsForBroker(OffsetReaderFactory.BROKER_LOG));
    }

    public void testKafkaNewConsumerOffsetReader() throws Exception {
        testOffsetReader(getParamsForBroker(OffsetReaderFactory.CONSUMER_NEW));
    }

    public void testKafka08ConsumerOffsetReader() throws Exception {
        testOffsetReader(getParamForZk(OffsetReaderFactory.CONSUMER_08, "kafka"));
    }

    public void testCamusOffsetReader() throws Exception {
        testOffsetReader(getParamForZk(OffsetReaderFactory.CAMUS, "camus"));
    }

    public void testKafkaSpoutOffsetReader() throws Exception {
        testOffsetReader(getParamForZk(OffsetReaderFactory.SPOUT, "stormetl"));
    }

    public void testOffsetReader(OffsetReaderParams params) throws Exception{
        IOffsetReader iOffsetReader = OffsetReaderFactory.create(params);
        testReadKafkaOffset(iOffsetReader);
        iOffsetReader.close();
    }

    private OffsetReaderParams getParamsForBroker(String type) {
        OffsetReaderParams params = new OffsetReaderParams();
        params.setType(type);
        params.setBrokers(broker);
        return params;
    }

    public OffsetReaderParams getParamForZk(String type, String namespace) {
        OffsetReaderParams params = new OffsetReaderParams();
        params.setType(type);
        params.setZookeeper(zk);
        params.setZkPath(namespace);
        return params;
    }

    public void testReadKafkaOffset(IOffsetReader iOffsetReader) throws Exception{
        KafkaTopicOffset readOffset = iOffsetReader.readOffset(group, topic);
        System.out.println(readOffset);
        checkOffset(readOffset);
    }

    public void checkOffset(KafkaTopicOffset kafkaTopicOffset) {
        assertNotNull(kafkaTopicOffset.getName());
        assertNotNull(kafkaTopicOffset.getGroup());
        assertNotNull(kafkaTopicOffset.getOffsets());
        assertTrue(kafkaTopicOffset.getOffsets().size() > 0);
    }

}
