package common;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class Consumer {

    public static KafkaConsumer<String, String> createConsumer() {
        String kafkaBootstrapServers = Constants.kafkaBootstrapServers;
        String zookeeperGroupId = Constants.zookeeperGroupId;
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        consumerProperties.put("group.id", zookeeperGroupId);
        consumerProperties.put("auto.commit.enable", "true");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(consumerProperties);
    }
}
