import common.Producer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static common.Consumer.createConsumer;
import static java.util.Collections.singletonList;

class EvenNumbersFilterer {

    private KafkaConsumer<String, String> kafkaConsumer;
    private Producer producer;

    private EvenNumbersFilterer(String topic) {
        kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(singletonList(topic));
        producer = new Producer();
    }

    private void filterEvenFromFibonacciStream() {

        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                String message = record.value();

                if(Integer.parseInt(message)%2 == 0) {
                    System.out.println("publishing messsage"+ message);
                    producer.publishEvent(message, "fibo.even");
                }
            }
        }
    }

    public static void main (String args[])
    {
        EvenNumbersFilterer evenNumbersFilterer = new EvenNumbersFilterer("fibo");
        evenNumbersFilterer.filterEvenFromFibonacciStream();
    }
}
