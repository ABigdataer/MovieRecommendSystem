/*
 * Copyright (c) 2017. WuYufei All rights reserved.
 */

package kafkaStreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {
    public static void main(String[] args){

        if (args.length < 4) {
            System.err.println("Usage: kafkaStream <brokers> <zookeepers> <from> <to>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <zookeepers> is a list of one or more Zookeeper nodes\n" +
                    "  <from> is a topic to consume from\n" +
                    "  <to> is a topic to product to\n\n");
            System.exit(1);
        }
        String brokers = args[0];//mini5:9092
        String zookeepers = args[1];//mini5:2181
        String from = args[2];//log
        String to = args[3];//recommender

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        StreamsConfig config = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", from)
                .addProcessor("PROCESS", () -> new LogProcess(), "SOURCE")
                .addSink("SINK", to, "PROCESS");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
