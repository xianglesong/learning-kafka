package com.xianglesong.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerConfirm {

  private final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
  public final static String TOPIC = "TEST-TOPIC";

  private KafkaProducerConfirm() {
    Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

    //request.required.acks
    //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
    //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
    //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
//        props.put("request.required.acks","-1");

    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
  }

  void produce() {
    int messageNo = 1;
    final int COUNT = 3;

    int i = 1;
    while (messageNo < COUNT) {
      String key = String.valueOf(messageNo);
      String data = "hello kafka message " + key;
      boolean sync = true;   //是否同步

      if (sync) {
        try {
          producer.send(new ProducerRecord<String, String>(TOPIC, data)).get();
          RecordMetadata metadata = producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i),
              Integer.toString(i))).get();
          System.out.println("message sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());

          producer.send(new ProducerRecord<String, String>("topic0", "message 2"), new Callback() {

            public void onCompletion(RecordMetadata metadata, Exception exception) {
              if (exception != null) {
                System.out.println("send message2 failed with " + exception.getMessage());
              } else {
                // offset 是消息在 partition 中的编号，可以根据 offset 检索消息
                System.out.println(
                    "message2 sent to " + metadata.topic() + ", partition " + metadata.partition()
                        + ", offset " + metadata.offset());
              }

            }

          });

        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        producer.send(new ProducerRecord<String, String>(TOPIC, data));
      }

      //必须写下面这句,相当于发送
      producer.flush();

      messageNo++;
    }
  }

  public static void main(String[] args) {
    new KafkaProducerConfirm().produce();
  }

}
