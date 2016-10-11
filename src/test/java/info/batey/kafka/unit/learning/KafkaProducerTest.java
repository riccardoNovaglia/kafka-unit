package info.batey.kafka.unit.learning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static info.batey.kafka.unit.learning.helper.KafkaClientHelper.getConsumer;
import static info.batey.kafka.unit.learning.helper.KafkaClientHelper.getProducer;
import static info.batey.kafka.unit.learning.helper.KafkaClientHelper.getProducerRecord;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


public class KafkaProducerTest {

    final String topic_name = "my-test-topic";

    private final int zookeeper_port_1 = 2181;
    private final int zookeeper_port_2 = 2182;
    private final int broker_port_1 = 9092;
    private final int broker_port_2 = 9093;

    final ServerUtils serverUtils = new ServerUtils();
    final ServerUtils serverUtils_2 = new ServerUtils();
    final ExecutorService executorService = Executors.newFixedThreadPool(2);


    @Before
    public void before() {
        serverUtils.startZookeeper(zookeeper_port_1);
        serverUtils.startBroker(broker_port_1, "1");
        serverUtils.createTopic(topic_name, 1);
        serverUtils_2.startZookeeper(zookeeper_port_2);
        serverUtils_2.startBroker(broker_port_2, "1");
        serverUtils_2.createTopic(topic_name, 1);
    }

    @After
    public void after() throws InterruptedException {
        serverUtils.stopBrokers();
        serverUtils.stopZookeeper();
        serverUtils_2.stopBrokers();
        serverUtils_2.stopZookeeper();
        executorService.shutdown();
    }

    @Test
    public void whenNewProducerIsCreatedForEachRequestThenBrokerAreChosenFromListOfBrokerAndMessagesSentUniformly()
        throws InterruptedException, ExecutionException {
        // given
        KafkaConsumer<String, String> consumer1 = getConsumer(broker_port_1, "test-group-1");
        KafkaConsumer<String, String> consumer2 = getConsumer(broker_port_2, "test-group-2");
        consumer1.subscribe(asList(topic_name));
        consumer2.subscribe(asList(topic_name));

        // when
        sendMessageToTopicUsingMultipleProducer(topic_name, 100);

        // then
        final Future<List<ConsumerRecord<String, String>>> consumerRecords1 =
            executorService.submit(new ConsumerThread(consumer1));
        final Future<List<ConsumerRecord<String, String>>> consumerRecords2 =
            executorService.submit(new ConsumerThread(consumer2));

        while (!consumerRecords1.isDone() && !consumerRecords2.isDone()) {
            Thread.sleep(2000L);
        }

        assertThat(isWithinThreshold(consumerRecords1.get().size(), consumerRecords2.get().size(), 20), is(TRUE));
    }


    @Test
    public void whenSingleProducerIsUsedForAllRequestsThenOneBrokerGetsAllMessages()
        throws InterruptedException, ExecutionException {

        final int numMessages = 100;

        // given
        KafkaConsumer<String, String> consumer1 = getConsumer(broker_port_1, "test-group-1");
        KafkaConsumer<String, String> consumer2 = getConsumer(broker_port_2, "test-group-2");
        consumer1.subscribe(asList(topic_name));
        consumer2.subscribe(asList(topic_name));

        // when
        sendMessageToTopicUsingSingleProducer(topic_name, numMessages);

        // then
        final Future<List<ConsumerRecord<String, String>>>
            consumerRecords1 = executorService.submit(new ConsumerThread(consumer1));
        final Future<List<ConsumerRecord<String, String>>> consumerRecords2 =
            executorService.submit(new ConsumerThread(consumer2));

        while (!consumerRecords1.isDone() && !consumerRecords2.isDone()) {
            Thread.sleep(2000L);
        }

        if (consumerRecords1.get().isEmpty()) {
            assertThat(consumerRecords2.get().size(), is(equalTo(numMessages)));
        } else if (consumerRecords2.get().isEmpty()) {
            assertThat(consumerRecords1.get().size(), is(equalTo(numMessages)));
        } else {
            fail("only one broker should receive messages not both");
        }
    }

    private Boolean isWithinThreshold(int left, int right, int threshold) {
        int total = left + right;
        if (left < right) {
            return (left - right) / total < threshold;
        } else {
            return (right - left) / total < threshold;
        }
    }

    private void sendMessageToTopicUsingSingleProducer(String topic, int noOfMessages) {
        try (KafkaProducer<String, String> producer = getProducer(broker_port_1, broker_port_2)) {
            for (int i = 0; i < noOfMessages; i++) {
                producer.send(getProducerRecord(topic, i));
            }
        }
    }

    private void sendMessageToTopicUsingMultipleProducer(String topic, int noOfMessages) {
        for (int i = 0; i < noOfMessages; i++) {
            try (KafkaProducer<String, String> producer = getProducer(broker_port_1, broker_port_2)) {
                producer.send(getProducerRecord(topic, i));
            }
        }
    }
}
