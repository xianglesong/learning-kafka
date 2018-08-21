package com.xianglesong.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerTransaction {

  public static void main(String[] args) {
    Properties props = new Properties();

    props.put("bootstrap.servers", "localhost:9092");
//    props.put("acks", "all");
    props.put("transactional.id", "my-transactional-id");
//    props.put("retries", 0);
//    props.put("batch.size", 16384);
//    props.put("linger.ms", 1);
//    props.put("buffer.memory", 33554432);
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props,
        new StringSerializer(), new StringSerializer());

    producer.initTransactions();

    try {
      producer.beginTransaction();
      for (int i = 0; i < 100; i++) {
        producer.send(new ProducerRecord<String, String>("my-topic-trans", Integer.toString(i),
            Integer.toString(i)));
      }
      producer.commitTransaction();
    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
      // We can't recover from these exceptions, so our only option is to close the producer and exit.
      producer.close();
    } catch (KafkaException e) {
      // For all other exceptions, just abort the transaction and try again.
      producer.abortTransaction();
    }
    producer.close();
  }

}
