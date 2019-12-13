import common.Producer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static common.Consumer.createConsumer;
import static java.util.Collections.singletonList;

class Adder {

    private KafkaConsumer<String, String> kafkaConsumer;
    Producer producer;

    private Adder(String topic) {
        kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(singletonList(topic));
        producer = new Producer();
    }

    private void addFilteredEvenFromFibonacciStream() {

        int sum = 0;
        Boolean continueLoop = Boolean.TRUE;

        while(continueLoop) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                String message = record.value();

                if (sum > 4000000) {
                    producer.publishEvent("stop", "stop");
                    continueLoop = Boolean.FALSE;
                    break;
                }
                sum = sum + Integer.parseInt(message);
            }
        }
    }

    public static void main (String args[])
    {
        Adder adder = new Adder("fibo.even");
        adder.addFilteredEvenFromFibonacciStream();
    }
}
