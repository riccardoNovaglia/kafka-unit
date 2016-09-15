package info.batey.kafka.unit;


import info.batey.kafka.unit.config.CertStoreConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ComparisonFailure;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static info.batey.kafka.unit.config.KafkaUnitConfig.BROKER_INTER_BROKER_PROTOCOL;
import static info.batey.kafka.unit.config.KafkaUnitConfig.BROKER_LISTENERS;
import static info.batey.kafka.unit.config.KafkaUnitConfig.CLIENT_SECURITY_PROTOCOL;
import static info.batey.kafka.unit.utils.FileUtils.registerDirectoriesToDelete;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_CLIENT_AUTH_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;

public class KafkaUnitWithSSL extends AbstractKafkaUnit {

    private final CertStoreConfig certStoreConfig;
    private final String DEFAULT_SERVER_KEYSTORE = "server.keystore.jks";
    private final String DEFAULT_SERVER_TRUSTSTORE = "server.truststore.jks";
    private final String DEFAULT_CLIENT_TRUSTSTORE = "client.truststore.jks";
    private final String DEFAULT_CLIENT_KEYSTORE = "client.keystore.jks";

    public KafkaUnitWithSSL(int zkPort, int brokerPort) {
        super();
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperUri = DEFAULT_HOST + ":" + zkPort;
        this.brokerString = DEFAULT_HOST + ":" + brokerPort;

        String defaultPassword = "test1234";
        this.certStoreConfig = new CertStoreConfig(
            getLocalCertStorePath(),
            defaultPassword,
            defaultPassword,
            defaultPassword,
            defaultPassword
        );
    }

    public KafkaUnitWithSSL(int zkPort, int brokerPort, CertStoreConfig certStoreConfig) {
        this.zkPort = zkPort;
        this.brokerPort = brokerPort;
        this.zookeeperUri = DEFAULT_HOST + ":" + zkPort;
        this.brokerString = DEFAULT_HOST + ":" + brokerPort;
        this.certStoreConfig = certStoreConfig;
    }

    @Override Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(BOOTSTRAP_SERVERS_CONFIG, brokerString);
        addSSLConfig(props);
        return props;
    }

    @Override protected void setBrokerConfig() {
        super.setBrokerConfig();
        kafkaBrokerConfig.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, certStoreConfig.getCertStoreDirectory() + File.separator + DEFAULT_SERVER_KEYSTORE);
        kafkaBrokerConfig.setProperty(SSL_TRUSTSTORE_LOCATION_CONFIG, certStoreConfig.getCertStoreDirectory() + File.separator + DEFAULT_SERVER_TRUSTSTORE);
        kafkaBrokerConfig.setProperty(SSL_KEYSTORE_PASSWORD_CONFIG, certStoreConfig.getServerKeystorePassword());
        kafkaBrokerConfig.setProperty(SSL_TRUSTSTORE_PASSWORD_CONFIG, certStoreConfig.getServerTruststorePassword());
        kafkaBrokerConfig.setProperty(SSL_KEY_PASSWORD_CONFIG, certStoreConfig.getServerKeystorePassword());
        kafkaBrokerConfig.setProperty(SSL_CLIENT_AUTH_CONFIG, "required");
        kafkaBrokerConfig.setProperty(BROKER_LISTENERS, format("SSL://" + DEFAULT_HOST + ":%d", brokerPort));
        kafkaBrokerConfig.setProperty(BROKER_INTER_BROKER_PROTOCOL, "SSL");
    }

    public ConsumerRecords<String, String> readMessages(String topicName, final int expectedMessages, long pollTimeoutInMs) {
        try (KafkaConsumer<String, String> consumer = getNewConsumer();) {
            consumer.subscribe(singletonList(topicName));
            final ConsumerRecords<String, String> records = consumer.poll(pollTimeoutInMs);
            if (records.count() != expectedMessages) {
                throw new ComparisonFailure("Incorrect number of messages returned",
                    Integer.toString(expectedMessages),
                    Integer.toString(records.count())
                );
            }
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Received message: {}", record.value());
            }
            return records;
        }
    }

    public ConsumerRecords<String, String> readMessages(String topicName, final int expectedMessages) {
        return readMessages(topicName, expectedMessages, timeout_3_Seconds);
    }

    @Override protected Properties getConsumerProperties() {
        final Properties properties = super.getConsumerProperties();
        addSSLConfig(properties);
        return properties;
    }

    private void addSSLConfig(Properties props) {
        props.put(CLIENT_SECURITY_PROTOCOL, "SSL");
        props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, certStoreConfig.getCertStoreDirectory() + File.separator + DEFAULT_CLIENT_TRUSTSTORE);
        props.put(SSL_KEYSTORE_LOCATION_CONFIG, certStoreConfig.getCertStoreDirectory() + File.separator + DEFAULT_CLIENT_KEYSTORE);
        props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, certStoreConfig.getClientTruststorePassword());
        props.put(SSL_KEYSTORE_PASSWORD_CONFIG, certStoreConfig.getClientKeystorePassword());
    }

    public String getClientKeystorePath(){
        return String.format("%s/" + DEFAULT_CLIENT_KEYSTORE, certStoreConfig.getCertStoreDirectory());
    }

    public String getClientTruststorePath(){
        return String.format("%s/" + DEFAULT_SERVER_KEYSTORE, certStoreConfig.getCertStoreDirectory());
    }

    public String getClientKeystorePassword(){
        return certStoreConfig.getClientKeystorePassword();
    }

    public String getClientTruststorePassword(){
        return certStoreConfig.getClientTruststorePassword();
    }

    private String getLocalCertStorePath() {
        final File certDir;
        try {
            certDir = java.nio.file.Files.createTempDirectory("certDir").toFile();
            Files.copy(getCertFiles(DEFAULT_CLIENT_KEYSTORE), Paths.get(certDir + File.separator + DEFAULT_CLIENT_KEYSTORE));
            Files.copy(getCertFiles(DEFAULT_CLIENT_TRUSTSTORE), Paths.get(certDir + File.separator + DEFAULT_CLIENT_TRUSTSTORE));
            Files.copy(getCertFiles(DEFAULT_SERVER_KEYSTORE), Paths.get(certDir + File.separator + DEFAULT_SERVER_KEYSTORE));
            Files.copy(getCertFiles(DEFAULT_SERVER_TRUSTSTORE), Paths.get(certDir + File.separator + DEFAULT_SERVER_TRUSTSTORE));

            registerDirectoriesToDelete(certDir);
        } catch (IOException e) {
            throw new RuntimeException("unable to create certificates directory", e);
        }
        return certDir.getPath();
    }

    private InputStream getCertFiles(String fileName){
        return this.getClass().getResourceAsStream(format("/certStore/%s", fileName));
    }
}
