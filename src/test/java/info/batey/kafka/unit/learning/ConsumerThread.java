package info.batey.kafka.unit.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

class ConsumerThread implements Callable<List<ConsumerRecord<String, String>>> {
    private final KafkaConsumer consumer;

    ConsumerThread(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public List<ConsumerRecord<String, String>> call() throws Exception {
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        try {
            final long timeOut = 5000L;
            final long endTime = System.currentTimeMillis() + timeOut;

            while (System.currentTimeMillis() < endTime) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    allRecords.add(consumerRecord);
                }
            }
        } finally {
            consumer.close();
        }

        return allRecords;
    }
}