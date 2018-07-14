package com.xianglesong.kafka;


/**
 * Created by rollin on 18/3/23.
 */
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class KafkaProducerBackup {

    private final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    public final static String TOPIC = "TEST-TOPIC";

    private KafkaProducerBackup(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
//        props.put(ProducerConfig.ACKS_CONFIG)

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
//        props.put("request.required.acks","-1");

        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    }

    void produce() {
        int messageNo = 1;
        final int COUNT = 2;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            boolean sync = false;   //是否同步

            if (sync) {
                try {
                    producer.send(new ProducerRecord<String, String>(TOPIC, data)).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                producer.send(new ProducerRecord<String, String>(TOPIC, data));
            }

            //必须写下面这句,相当于发送
            producer.flush();

            messageNo ++;
        }
    }

    public static void main( String[] args ) {
        new KafkaProducerBackup().produce();
    }

}
