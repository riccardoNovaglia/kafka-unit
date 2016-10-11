package info.batey.kafka.unit.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static info.batey.kafka.unit.learning.helper.KafkaClientHelper.getConsumer;
import static info.batey.kafka.unit.learning.helper.KafkaClientHelper.getProducer;
import static info.batey.kafka.unit.learning.helper.KafkaClientHelper.getProducerRecord;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


public class KafkaClusterTest {

    final String topic_name = "my-test-topic";

    private final int zookeeper_port_1 = 2181;
    private final int broker_port_1 = 9092;
    private final int broker_port_2 = 9093;

    final ServerUtils serverUtils = new ServerUtils();
    final ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Before
    public void before() {
        serverUtils.startZookeeper(zookeeper_port_1);
        serverUtils.startBroker(broker_port_1, "1");
        serverUtils.startBroker(broker_port_2, "2");
        serverUtils.createTopic(topic_name, 10);
    }

    @After
    public void after() throws InterruptedException {
        serverUtils.stopBrokers();
        serverUtils.stopZookeeper();
        executorService.shutdown();
    }


    @Test
    public void topicLoadIsBalancedAcrossMultipleKafkaConsumersWithinSameGroup() throws Exception {
        // given
        KafkaConsumer<String, String> consumer1 = getConsumer(broker_port_1, "test-group-1");
        KafkaConsumer<String, String> consumer2 = getConsumer(broker_port_2, "test-group-1");
        consumer1.subscribe(asList(topic_name));
        consumer2.subscribe(asList(topic_name));

        // when
        sendMessagesTo(topic_name, 100);

        final Future<List<ConsumerRecord<String, String>>> consumerRecords1 =
            executorService.submit(new ConsumerThread(consumer1));
        final Future<List<ConsumerRecord<String, String>>> consumerRecords2 =
            executorService.submit(new ConsumerThread(consumer2));

        while (!consumerRecords1.isDone() && !consumerRecords2.isDone()) {
            Thread.sleep(2000L);
        }

        assertThat(consumerRecords1.get().size(), is(greaterThanOrEqualTo(45)));
        assertThat(consumerRecords2.get().size(), is(greaterThanOrEqualTo(45)));
    }

    private void sendMessagesTo(String topic, int noOfMessages) {
        try (KafkaProducer<String, String> producer = getProducer(broker_port_1, broker_port_2)) {
            for (int i = 0; i < noOfMessages; i++) {
                producer.send(getProducerRecord(topic, i));
            }
        }
    }
}
