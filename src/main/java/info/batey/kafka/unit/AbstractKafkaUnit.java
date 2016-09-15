package info.batey.kafka.unit;

import com.jayway.awaitility.Duration;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.hamcrest.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.jayway.awaitility.Awaitility.await;
import static info.batey.kafka.unit.config.KafkaUnitConfig.BROKER_HOST_NAME;
import static info.batey.kafka.unit.config.KafkaUnitConfig.BROKER_ID;
import static info.batey.kafka.unit.config.KafkaUnitConfig.BROKER_PORT;
import static info.batey.kafka.unit.config.KafkaUnitConfig.LOG_CLEANER_ENABLE;
import static info.batey.kafka.unit.config.KafkaUnitConfig.OFFSETS_TOPIC_NUM_PARTITIONS;
import static info.batey.kafka.unit.config.KafkaUnitConfig.ZOOKEEPER_CONNECT;
import static info.batey.kafka.unit.config.KafkaUnitConfig.ZOOKEEPER_LOG_DIRECTORY;
import static info.batey.kafka.unit.config.KafkaUnitConfig.ZOOKEEPER_LOG_FLUSH_INTERVAL_MESSAGES;
import static info.batey.kafka.unit.utils.FileUtils.registerDirectoriesToDelete;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;


public abstract class AbstractKafkaUnit {

    protected final String DEFAULT_HOST = "localhost";
    protected static final Logger LOGGER = LoggerFactory.getLogger(KafkaUnit.class);
    protected KafkaServer broker;
    protected Zookeeper zookeeper;
    protected String zookeeperUri;
    protected String brokerString;
    protected int zkPort;
    protected int brokerPort;
    protected int timeout_3_Seconds = 3000;
    protected Properties kafkaBrokerConfig = new Properties();

    abstract Properties getProducerConfig();

    protected void setBrokerConfig() {
        final File logDir = getLogDirectory();
        kafkaBrokerConfig.setProperty(ZOOKEEPER_CONNECT, zookeeperUri);
        kafkaBrokerConfig.setProperty(BROKER_ID, "1");
        kafkaBrokerConfig.setProperty(BROKER_HOST_NAME, "localhost");
        kafkaBrokerConfig.setProperty(BROKER_PORT, Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty(ZOOKEEPER_LOG_DIRECTORY, logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty(ZOOKEEPER_LOG_FLUSH_INTERVAL_MESSAGES, valueOf(1));
        kafkaBrokerConfig.setProperty(LOG_CLEANER_ENABLE, "false");
        kafkaBrokerConfig.setProperty(OFFSETS_TOPIC_NUM_PARTITIONS, "1");
    }

    public void startup() {
        setBrokerConfig();
        zookeeper = new Zookeeper(zkPort);
        zookeeper.startup();
        final KafkaConfig serverConfig = new KafkaConfig(kafkaBrokerConfig);
        broker = new KafkaServer(serverConfig, now(), Option.<String>empty());
        broker.startup();
    }

    @SafeVarargs
    public final void sendMessages(ProducerRecord<String, String> message, ProducerRecord<String, String>... messages) {

        try (Producer<String, String> producer = createProducer();) {
            producer.send(message);
            for (ProducerRecord<String, String> msg : messages) {
                producer.send(msg);
            }
        }
    }

    private Producer<String, String> createProducer() {
        return new KafkaProducer<>(getProducerConfig());
    }

    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }

    public void createTopic(String topicName, Integer numPartitions) {
        // setup
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperUri;
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "" + Integer.valueOf(numPartitions);
        arguments[7] = "--topic";
        arguments[8] = topicName;
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
            30000, 30000, JaasUtils.isZkSecurityEnabled()
        );

        // run
        try {
            LOGGER.info("Executing: CreateTopic " + Arrays.toString(arguments));
            TopicCommand.createTopic(zkUtils, opts);
        } finally {
            zkUtils.close();
        }

        try (final KafkaConsumer<String, String> consumer = getNewConsumer()) {
            await().atMost(Duration.FIVE_SECONDS).until(new Callable<Set<String>>() {
                @Override public Set<String> call() throws Exception {
                    return consumer.listTopics().keySet();
                }
            }, Matchers.hasItem(topicName));
        }
    }

    /* Set custom broker configuration.
     * See avaliable config keys in the kafka documentation: http://kafka.apache.org/documentation
     * .html#brokerconfigs
     */
    public final void setKafkaBrokerConfig(String configKey, String configValue) {
        kafkaBrokerConfig.setProperty(configKey, configValue);
    }

    public String getKafkaConnect() {
        return brokerString;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void shutdown() {

        if (broker != null) {
            broker.shutdown();
        }
        if (zookeeper != null) {
            zookeeper.shutdown();
        }
    }

    protected KafkaConsumer<String,String> getNewConsumer() {
        return new KafkaConsumer<>(getConsumerProperties());
    }

    protected Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, format(DEFAULT_HOST + ":%d", brokerPort));
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(GROUP_ID_CONFIG, "kafka-unit-group");
        return props;
    }

    protected static int parseConnectionString(String connectionString) {
        try {
            String[] hostPorts = connectionString.split(",");

            if (hostPorts.length != 1) {
                throw new IllegalArgumentException("Only one 'host:port' pair is allowed in connection string");
            }

            String[] hostPort = hostPorts[0].split(":");

            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Invalid format of a 'host:port' pair");
            }

            if (!"localhost".equals(hostPort[0])) {
                throw new IllegalArgumentException("Only localhost is allowed for KafkaUnit");
            }

            return Integer.parseInt(hostPort[1]);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse connectionString " + connectionString, e);
        }
    }

    private static int getEphemeralPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    protected File getLogDirectory() {
        final File logDir;
        try {
            logDir = Files.createTempDirectory("kafka").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka", e);
        }

        registerDirectoriesToDelete(logDir);
        return logDir;
    }

    private Time now() {
        return new Time() {
            @Override
            public long milliseconds() {
                return System.currentTimeMillis();
            }

            @Override
            public long nanoseconds() {
                return System.nanoTime();
            }

            @Override
            public void sleep(long ms) {
                try {
                    Thread.sleep(ms);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
