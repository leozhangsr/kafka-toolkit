package com.bigdatalighter.kafka.args;

import org.kohsuke.args4j.Option;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaReadMainOption {

    @Option(name="--brokers", aliases = "-b",  usage = "kafka broker list, host:port", required = true)
    public String brokers;

    @Option(name="--topic", aliases = "-t", usage = "kafka topic name", required = true)
    public String topic;

    @Option(name="--partition", aliases = "-p", usage = "partitionId")
    public int partition = 0;

    @Option(name="--offset", aliases = "-o", usage = "offset to read from,[-1:latest,-2,earliest], default read from latest")
    public long offset = -2;

    @Option(name="--number", aliases = "-n", usage = "numbers of message to read")
    public long num = 10;

    @Option(name="--printbinary", aliases = "-pb" , usage = "print binary array")
    public boolean printBinary = false;

}
