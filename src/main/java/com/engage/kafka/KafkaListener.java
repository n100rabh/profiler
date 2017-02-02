package com.engage.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaListener {
  private final ConsumerConnector consumer;
  private final String topic;
  private ExecutorService executor;

  public KafkaListener(String a_zookeeper, String a_groupId, String a_topic) {
    consumer = kafka.consumer.Consumer
        .createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
    this.topic = a_topic;
  }

  public void shutdown() {
    if (consumer != null)
      consumer.shutdown();
    if (executor != null)
      executor.shutdown();
    try {
      if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        System.out
            .println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
      }
    } catch (InterruptedException e) {
      System.out.println("Interrupted during shutdown, exiting uncleanly");
    }
  }

  public void run(int a_numThreads) {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(a_numThreads));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

    // now launch all the threads
    //
    executor = Executors.newFixedThreadPool(a_numThreads);

    // now create an object to consume the messages
    //
    int threadNumber = 0;
    for (final KafkaStream stream : streams) {
      executor.submit(new ConsumedDataProcessor(stream, threadNumber));
      threadNumber++;
    }
  }

  private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
    Properties props = new Properties();
    props.put("zookeeper.connect", a_zookeeper);
    props.put("group.id", a_groupId);
    props.put("zookeeper.session.timeout.ms", "10000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);
  }

  public static void main(String[] args) {
    String zooKeeper = "75.126.75.197:2181,75.126.75.197:2182,75.126.75.197:2183";
    String groupId = "BootCamp";
    String topic = "analyze";
    int threads = 10;// Integer.parseInt(args[3]);

    KafkaListener example = new KafkaListener(zooKeeper, groupId, topic);
    example.run(threads);

    // Wait forever...
    while (true) {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        example.shutdown();
        System.out.println("Main thread interrupted!!");
      }
    }
  }
}
