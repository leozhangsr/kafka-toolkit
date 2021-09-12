# kafka-toolkit
A toolkit for kafka maintenance and development

# IOffsetReader
An interface to read kafka offset from different consumer or broker directly.
Serveral implements are supplied for common use cases, including kakfa cunsumer api for 0.8 and 1.0 above(with dependence of 2.4), kafka broker log earliest/latest offset,KafkaSpout,and camus.
We can use this interface to monitor kafka consumer.A runner is supplied to use to read the offset by command line.
```
#java -cp offset-kit-1.0-SNAPSHOT.jar com.bigdatalighter.kafka.ReadKafkaOffset
Usage: [--brokers (-b) VAL] [--fi VAL] [--from (-f) VAL] --group (-g) VAL --topic (-tp) VAL --type (-t) VAL 
       [--zk-path VAL] [--zookeeper (-zk) VAL] [-fo VAL]
Description:
 --brokers (-b) VAL    : Kafka brokers in the form host:port
 --fi VAL              : File to read from
 --from (-f) VAL       : From earliest/latest offset, only supported by
                         type:broker_log (default: latest)
 --group (-g) VAL      : Consumer group
 --topic (-tp) VAL     : Topic to read
 --type (-t) VAL       : Type of OffsetReader,known type:consumer08,consumer-new
                         ,broker-log,spout,camus,json-file
 --zk-path VAL         : Namespace path for zookeeper
 --zookeeper (-zk) VAL : Zookeeper connection in the form host:port
 -fo VAL               : File to store offset
```

# IOffsetWriter
An interface to store or overwrite kafka consumer offset.
Serveral implements are supplied for common use cases, including kakfa cunsumer api for 0.8,KafkaSpout,and camus.
A runner is supplied to use to overwrite kafka consumer offset by command line.It reads offset from a json file,then overwrite it to a specific consumer.Only use it when you want to change the offset manually.
```
#java -cp offset-kit-1.0-SNAPSHOT.jar com.bigdatalighter.kafka.WriteKafkaOffset
Usage: [--brokers (-b) VAL] --type (-t) VAL [--zk-path VAL] [--zookeeper (-zk) VAL] -fi VAL
Description:
 --brokers (-b) VAL    : Kafka brokers in the form host:port
 --type (-t) VAL       : Type of OffsetWriter, known type:consumer08,camus,spout
                         ,json-file
 --zk-path VAL         : Namespace path for zookeeper
 --zookeeper (-zk) VAL : Zookeeper connection in the form host:port
 -fi VAL               : File to read from
```

# Consumer offset monitor
A OffsetStateMonitor can monitor a consumer offset state, to see if a consumer can catch up with the target offset or not.If it does catch up with the target offset,we can choose to do something like trigger a dependent task.Or if can't, we can choose to trigger an alarm.To achieve these, you need to extends the AbsConsumerOffsetStateObserver and implement your own doCaughtUp() and doDelay() function.A LoggingOffsetStateObserver is supplied for base use.
You can make up a config file to use the monitor with supplied IOffsetReader.An use case is shown as following with a ![config file](https://github.com/leozhangsr/kafka-toolkit/blob/main/offset-kit/src/main/resources/test_monitor.config)
.
```
#java -cp offset-kit-1.0-SNAPSHOT.jar com.bigdatalighter.kafka.MonitorConsumerOffset test_monitor.config
Monitor config file is required.
Usage: configFile
```
