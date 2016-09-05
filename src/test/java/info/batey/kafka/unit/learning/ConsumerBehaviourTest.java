package info.batey.kafka.unit.learning;

import info.batey.kafka.unit.rules.KafkaUnitRule;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class ConsumerBehaviourTest {

    public static final int ONE_SECOND = 1000;
    public static final List<String> INITIAL_FIVE_MESSAGES = Arrays.asList("message0",
        "message1",
        "message2",
        "message3",
        "message4"
    );
    private final String DEFAULT_HOST = "localhost";

    @Rule
    public KafkaUnitRule rule = new KafkaUnitRule(5000, 5001);

    @Test
    public void consumer_offset_latest_ignores_messages_before_first_poll() throws Exception {
        // given
        givenFiveMessages();
        Consumer<String, String> consumer = getNewConsumerSubscribed("latest", "kafka-unit-group", "clientId");
        assertEquals(0, consumer.poll(0).count());

        // when
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "latest-message"));

        // then
        ConsumerRecords<String, String> consumerRecords1 = consumer.poll(ONE_SECOND);
        assertEquals(messagesAsString(consumerRecords1), 1, consumerRecords1.count());
        assertEquals(singletonList("latest-message"), messagesAsList(consumerRecords1.iterator()));
        assertEquals(0, consumer.poll(ONE_SECOND).count());
    }

    @Test
    public void poll_zero_triggers_async_buffer_fill_up_on_next_poll_call() throws Exception {
        // given
        givenFiveMessages();
        Consumer<String, String> consumer = getNewConsumerSubscribed("earliest", "kafka-unit-group", "clientId");
        consumer.subscribe(singletonList("topic1"));
        ConsumerRecords consumerRecords = consumer.poll(0);
        assertEquals(0, consumerRecords.count());

        // when
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "latest-message"));

        // then
        ConsumerRecords<String, String> consumerRecords2 = consumer.poll(0);
        assertEquals(INITIAL_FIVE_MESSAGES, messagesAsList(consumerRecords2.iterator()));

        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "even-later-message"));

        ConsumerRecords<String, String> consumerRecords3 = consumer.poll(0);
        assertEquals(messagesAsString(consumerRecords3), 1, consumerRecords3.count());
        ConsumerRecord consumerRecord3 = (ConsumerRecord) consumerRecords3.iterator().next();
        assertEquals("latest-message", consumerRecord3.value());

        Thread.sleep(ONE_SECOND); // allow time for buffer to fill up - no sendMessage to take time

        ConsumerRecords<String, String> consumerRecords4 = consumer.poll(0);
        assertEquals(messagesAsString(consumerRecords4), 1, consumerRecords4.count());
        ConsumerRecord consumerRecord4 = (ConsumerRecord) consumerRecords4.iterator().next();
        assertEquals("even-later-message", consumerRecord4.value());
    }

    @Test
    public void async_buffer_fill_does_take_time_on_first_use() throws Exception {
        // given
        givenFiveMessages();

        Consumer<String, String> consumer = getNewConsumerSubscribed("earliest", "kafka-unit-group", "clientId");
        assertEquals(0, consumer.poll(0).count());
        assertEquals(0, consumer.poll(0).count());
    }

    @Test
    public void async_buffer_fill_does_not_need_time_after_synchronous_producer() throws Exception {
        // given
        givenFiveMessages();
        Consumer<String, String> consumer = getNewConsumerSubscribed("earliest", "kafka-unit-group", "clientId");
        assertEquals(0, consumer.poll(0).count());

        Thread.sleep(ONE_SECOND);  // time for async to fill buffer

        ConsumerRecords<String, String> consumerRecords2 = consumer.poll(0);
        assertEquals(INITIAL_FIVE_MESSAGES,
            messagesAsList(consumerRecords2.iterator())
        );

        // when
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "latest-message"));

        // then
        ConsumerRecords consumerRecords3 = consumer.poll(0);
        // This may not be 100% fetched. It is asynchronous and may fail time to time.
        assertEquals(messagesAsString(consumerRecords3), 1, consumerRecords3.count());
        assertEquals("latest-message", ((ConsumerRecord) consumerRecords3.iterator().next()).value());
    }

    @Test
    public void async_buffer_fill_does_not_fill_if_a_delay_between_poll_and_synchronous_producer() throws Exception {
        // given
        givenFiveMessages();
        Consumer<String, String> consumer = getNewConsumerSubscribed("earliest", "kafka-unit-group", "clientId");
        assertEquals(0, consumer.poll(0).count());

        Thread.sleep(ONE_SECOND);  // time for async to fill buffer

        ConsumerRecords<String, String> consumerRecords2 = consumer.poll(0);
        assertEquals(INITIAL_FIVE_MESSAGES,
            messagesAsList(consumerRecords2.iterator())
        );

        // when
        Thread.sleep(ONE_SECOND);  // fast machine anything above 282 will still pass
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "latest-message"));

        // then
        assertEquals(0, consumer.poll(0).count());
    }

    @Test
    public void async_buffer_starts_to_fill_once_message_is_received_so_multiple_polls_might_be_needed()
        throws Exception {
        // given
        Consumer<String, String> consumer = getNewConsumerSubscribed("earliest", "kafka-unit-group", "clientId");
        assertEquals(0, consumer.poll(0).count());
        Thread.sleep(ONE_SECOND);  // time for async to fill buffer

        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "latest-message"));
        assertEquals(0, consumer.poll(0).count());
        Thread.sleep(ONE_SECOND);
        assertEquals(0, consumer.poll(0).count());
        Thread.sleep(ONE_SECOND);
        assertEquals(1, consumer.poll(0).count());
    }

    @Test
    public void offset_is_not_at_subscription_point() throws Exception {
        // given
        givenFiveMessages();
        Consumer<String, String> consumer = getNewConsumerSubscribed("latest", "kafka-unit-group", "clientId");

        // when
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "latest-message"));

        // then
        assertEquals(0, consumer.poll(ONE_SECOND).count());
    }

    @Test
    public void offset_latest_is_at_point_of_poll_and_only_producers_after_this_first_poll_are_found() {
        // given
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "message1"));
        Consumer<String, String> consumer = getNewConsumerSubscribed("latest", "kafka-unit-group", "clientId");

        // when
        assertEquals(0, consumer.poll(ONE_SECOND).count());
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "another-message"));

        // then
        ConsumerRecords<String, String> consumerRecords = consumer.poll(ONE_SECOND);
        assertEquals(1, consumerRecords.count());
        ConsumerRecord consumerRecord = (ConsumerRecord) consumerRecords.iterator().next();
        assertEquals("another-message", consumerRecord.value());
    }

    @Test
    public void offset_is_saved_between_consumers() throws Exception {
        // given
        givenFiveMessages();
        Consumer<String, String> consumer = getNewConsumerSubscribed("earliest", "kafka-unit-group", "clientId1");
        ConsumerRecords<String, String> consumerRecords = consumer.poll(ONE_SECOND);
        assertEquals(5, consumerRecords.count());
        consumer.commitSync();
        consumer.close();

        // when
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "latest-message"));

        // then
        Consumer<String, String> consumerSameGroup = getNewConsumerSubscribed("earliest",
            "kafka-unit-group",
            "clientId2"
        );
        consumerRecords = consumerSameGroup.poll(ONE_SECOND);
        assertEquals(1, consumerRecords.count());
        ConsumerRecord consumerRecord = (ConsumerRecord) consumerRecords.iterator().next();
        assertEquals("latest-message", consumerRecord.value());

        Consumer<String, String> consumerAnotherGroup = getNewConsumerSubscribed("earliest",
            "another-group",
            "clientId"
        );
        consumerRecords = consumerAnotherGroup.poll(ONE_SECOND);
        assertEquals(6, consumerRecords.count());
    }

    @Test(expected = NoOffsetForPartitionException.class)
    public void offset_none_fails_when_consumer_startup_if_consumer_group_never_seen() {
        // given
        rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "message1"));
        Consumer<String, String> consumer = getNewConsumerSubscribed("none", "kafka-unit-group", "clientId");

        // when
        consumer.poll(0);
    }

//  
    
    @Test
    public void wakeup_between_poll_will_not_consume_messages_from_queue() throws InterruptedException {

        final List<String> result = new ArrayList<>();

        final Consumer<String, String> consumerSameGroup = getNewConsumerSubscribed("latest",
            "kafka-unit-group",
            "clientId"
        );
        Runnable r = new Runnable() {
            @Override
            public void run() {
                //try {
                    while (true) {
                        //Thread.sleep(1000);
                        final ConsumerRecords<String, String> poll = consumerSameGroup.poll(1000);
                        for (ConsumerRecord<String, String> re : poll) {
                            result.add(re.value());
                        }
                    }
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        };

        final Thread thread = new Thread(r, "my-thread");
        thread.start();

//         rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "message1"));


        int i = 0;
        while(i < 100){
            rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "latest-message"));
            Thread.sleep(100);
            consumerSameGroup.wakeup();
            
            i ++;
        }


    }

    private String messagesAsString(ConsumerRecords<String, String> records) {
        return messagesAsList(records.iterator()).toString();
    }

    private List<String> messagesAsList(Iterator<ConsumerRecord<String, String>> iterator) {
        List<String> messages = new ArrayList<>();

        while (iterator.hasNext()) {
            messages.add(iterator.next().value());
        }
        return messages;
    }

    private void givenFiveMessages() {
        for (int i = 0; i < 5; i++) {
            rule.getKafkaUnit().sendMessages(new ProducerRecord<String, String>("topic1", "message" + i));
        }
    }

    private Consumer<String, String> getNewConsumerSubscribed(
        String offsetResetWhenNotSaved, String groupId, String clientId
    ) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, format(DEFAULT_HOST + ":%d", 5001));
        props.put(AUTO_OFFSET_RESET_CONFIG, offsetResetWhenNotSaved);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(CLIENT_ID_CONFIG, clientId);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(singletonList("topic1"));
        return consumer;

    }
}