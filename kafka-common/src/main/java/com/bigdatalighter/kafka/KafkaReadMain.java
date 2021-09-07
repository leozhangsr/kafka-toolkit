package com.bigdatalighter.kafka;

import com.bigdatalighter.kafka.args.KafkaReadMainOption;
import com.bigdatalighter.kafka.common.KafkaOffsetRetrievalFailureException;
import com.bigdatalighter.kafka.common.KafkaPartition;
import com.bigdatalighter.kafka.common.KafkaTopic;
import com.bigdatalighter.kafka.common.KafkaWrapper;
import com.bigdatalighter.kafka.utils.ArgsUtils;
import com.bigdatalighter.kafka.utils.ByteBufferUtils;
import kafka.message.MessageAndOffset;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaReadMain {

    public static void main(String[] args) throws Exception {
        KafkaReadMain kafkaReadMain = new KafkaReadMain();
        KafkaReadMainOption option = ArgsUtils.parseArgs(args, KafkaReadMainOption.class);
        kafkaReadMain.runReadKafka(option);
    }

    public KafkaReadMainOption parseArgs(String[] args) throws CmdLineException {
        final KafkaReadMainOption option = new KafkaReadMainOption();
        final CmdLineParser cmdLineParser = new CmdLineParser(option);
        if (args.length == 0) {
            System.out.print("Usage:");
            cmdLineParser.printSingleLineUsage(System.out);
            System.out.println("\r\nDescription:");
            cmdLineParser.printUsage(System.out);
            System.exit(0);
        }
        cmdLineParser.parseArgument(args);
        return option;
    }

    public void runReadKafka(KafkaReadMainOption option) throws Exception {
        final KafkaWrapper kafkaWrapper = new KafkaWrapper.Builder().withBrokers(option.brokers).build();
        List<Pattern> topics = new ArrayList<Pattern>();
        topics.add(Pattern.compile(option.topic));
        List<KafkaTopic> filteredTopics = kafkaWrapper.getFilteredTopics(new ArrayList<Pattern>(), topics);
        for (KafkaTopic topic : filteredTopics) {
            List<KafkaPartition> partitions = topic.getPartitions();
            for (KafkaPartition partition : partitions) {
                if (partition.getId() == option.partition) {
                    long startOffset = getStartOffset(option.offset, kafkaWrapper, partition);
                    long endOffset = startOffset + option.num;
                    Iterator<MessageAndOffset> messageAndOffsetIterator = kafkaWrapper.fetchNextMessageBuffer(partition, startOffset, endOffset);
                    if (messageAndOffsetIterator != null) {
                        while (messageAndOffsetIterator.hasNext()) {
                            MessageAndOffset mess = messageAndOffsetIterator.next();
                            if (mess.offset() <= endOffset) {
                                byte[] bytes = ByteBufferUtils.getBytes(mess.message().payload());
                                if (option.printBinary) {
                                    System.out.println(mess.offset() + "\t" + Arrays.toString(bytes));
                                } else {
                                    String s = new String(bytes);
                                    System.out.println(mess.offset() + "\t" + s);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private long getStartOffset(long offset, KafkaWrapper kafkaWrapper, KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
        if (offset == -1) {
            offset = kafkaWrapper.getLatestOffset(partition);
        } else if (offset == -2) {
            offset = kafkaWrapper.getEarliestOffset(partition);
        }
        return offset;
    }

}
