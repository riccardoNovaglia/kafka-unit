package info.batey.kafka.unit.learning;

import info.batey.kafka.unit.Zookeeper;
import kafka.admin.TopicCommand;
import kafka.common.TopicExistsException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.Time;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static info.batey.kafka.unit.config.KafkaUnitConfig.BROKER_HOST_NAME;
import static info.batey.kafka.unit.config.KafkaUnitConfig.BROKER_ID;
import static info.batey.kafka.unit.config.KafkaUnitConfig.BROKER_PORT;
import static info.batey.kafka.unit.config.KafkaUnitConfig.LOG_CLEANER_ENABLE;
import static info.batey.kafka.unit.config.KafkaUnitConfig.ZOOKEEPER_CONNECT;
import static info.batey.kafka.unit.config.KafkaUnitConfig.ZOOKEEPER_LOG_DIRECTORY;
import static info.batey.kafka.unit.config.KafkaUnitConfig.ZOOKEEPER_LOG_FLUSH_INTERVAL_MESSAGES;
import static info.batey.kafka.unit.utils.FileUtils.registerDirectoriesToDelete;
import static java.lang.String.valueOf;

public class ServerUtils {

    private static final String default_host = "localhost";
    private Zookeeper zookeeper;
    private String zookeeperUri;
    private List<KafkaServer> brokers = new ArrayList();

    public void startZookeeper(int port) {
        zookeeper = new Zookeeper(port);
        zookeeperUri = default_host + ":" + port;
        zookeeper.startup();
    }

    public void startBroker(int port, String id) {
        KafkaServer broker = new KafkaServer(getBrokerConfig(port, id), now(), Option.<String>empty());
        brokers.add(broker);
        broker.startup();
    }

    public void stopZookeeper() {
        if (zookeeper != null) {
            zookeeper.shutdown();
        }
    }

    public void stopBrokers() {
         for(KafkaServer broker:brokers){
             broker.shutdown();
         }
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
            System.out.println("Executing: CreateTopic " + Arrays.toString(arguments));
            TopicCommand.createTopic(zkUtils, opts);
        } catch (TopicExistsException e) {
            System.out.println(String.format("Topic '%s' already exists, not attempting to create it again. " +
                                             "This should not happen if KafkaUnit is successfully restarted between " +
                                             "tests.",
                topicName
            ));
        } finally {
            zkUtils.close();
        }
    }

    private KafkaConfig getBrokerConfig(int brokerPort, String id) {
        final File logDir = getLogDirectory();
        Properties kafkaBrokerConfig = new Properties();
        kafkaBrokerConfig.setProperty(ZOOKEEPER_CONNECT, zookeeperUri);
        kafkaBrokerConfig.setProperty(BROKER_ID, id);
        kafkaBrokerConfig.setProperty(BROKER_HOST_NAME, "localhost");
        kafkaBrokerConfig.setProperty(BROKER_PORT, Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty(ZOOKEEPER_LOG_DIRECTORY, logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty(ZOOKEEPER_LOG_FLUSH_INTERVAL_MESSAGES, valueOf(1));
        kafkaBrokerConfig.setProperty(LOG_CLEANER_ENABLE, "false");
       // kafkaBrokerConfig.setProperty(OFFSETS_TOPIC_NUM_PARTITIONS, "2");

        return new KafkaConfig(kafkaBrokerConfig);
    }

    private File getLogDirectory() {
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
        return SystemTime$.MODULE$;
    }
}


