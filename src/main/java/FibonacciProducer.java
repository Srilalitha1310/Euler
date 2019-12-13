import common.Producer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static common.Consumer.createConsumer;
import static java.util.Collections.singletonList;
import static common.Utils.fib;

class FibonacciProducer {

    private KafkaConsumer<String, String> kafkaConsumer;
    private Producer producer;

    private FibonacciProducer() {
        kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(singletonList("stop"));
        producer = new Producer();
    }

    private void produceFibonacci() {

        int number = 0;

        while(true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            if (!records.isEmpty()) {
                break;
            }
            String message = String.valueOf(fib(number));
            System.out.println("Producing fibonacci" + message);
            producer.publishEvent(message,"fibo");
            number++;
        }
    }

    public static void main (String args[])
    {
        FibonacciProducer fibonacciProducer = new FibonacciProducer();
        fibonacciProducer.produceFibonacci();
    }

}
