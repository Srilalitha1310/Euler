package common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    private KafkaProducer<String, String> kafkaProducer;

    public Producer() {
        String kafkaBootstrapServers = Constants.kafkaBootstrapServers;
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<>(producerProperties);
    }

    public void publishEvent(String message, String topic) {
        System.out.println("Sending message to topic: " + topic);
        kafkaProducer.send(new ProducerRecord<>(topic, message));
    }
}
