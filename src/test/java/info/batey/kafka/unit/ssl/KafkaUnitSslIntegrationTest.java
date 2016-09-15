/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.batey.kafka.unit.ssl;

import info.batey.kafka.unit.rules.KafkaUnitRuleWithSSL;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static info.batey.kafka.unit.config.KafkaUnitConfig.CLIENT_SECURITY_PROTOCOL;
import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.junit.Assert.assertTrue;

public class KafkaUnitSslIntegrationTest {

    private static final int ZOOKEEPER_PORT = 6000;
    private static final int BROKER_PORT = 6001;

    @Rule
    public KafkaUnitRuleWithSSL kafkaUnitSslRule = new KafkaUnitRuleWithSSL(ZOOKEEPER_PORT, BROKER_PORT);

    @Test
    public void junitRuleShouldHaveStartedKafkaWithSsl() throws Exception {
        //given
        Random random = new Random();
        String testTopic = "test-topic";
        kafkaUnitSslRule.getKafkaUnit().createTopic(testTopic);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(testTopic,
            "key",
            "test value :" + random.nextLong()
        );

        //when
        kafkaUnitSslRule.getKafkaUnit().sendMessages(producerRecord);

        // then
       kafkaUnitSslRule.getKafkaUnit().readMessages(testTopic, 1);

    }

    @Test
    public void shouldCreateTopic() throws Exception {
        kafkaUnitSslRule.getKafkaUnit().createTopic("test-topic");

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties())) {
            final Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            assertTrue(topics.containsKey("test-topic"));
        }
    }

    private Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, format("localhost" + ":%d", BROKER_PORT));
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(GROUP_ID_CONFIG, "kafka-unit-group");
        properties.put(CLIENT_SECURITY_PROTOCOL, "SSL");
        properties.put(SSL_TRUSTSTORE_LOCATION_CONFIG, "./src/main/resources/certStore/client.truststore.jks");
        properties.put(SSL_KEYSTORE_LOCATION_CONFIG, "./src/main/resources/certStore/client.keystore.jks");
        properties.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        properties.put(SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        return properties;
    }
}
