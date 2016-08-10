package info.batey.kafka.unit.ssl;

import info.batey.kafka.unit.CertStoreConfig;
import info.batey.kafka.unit.KafkaUnitWithSSL;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KafkaSSLUnitTest {

    @Test
    public void testCertificateStoreConfigIsSetCorrectly() throws Exception {
        final CertStoreConfig certStoreConfig = new CertStoreConfig(
            "/path/to/certs",
            "clientKeystorePassword",
            "clientTruststorePassword",
            "serverKeystorePassword",
            "serverTruststorePassword"
        );
        KafkaUnitWithSSL kafkaUnitWithSSL = new KafkaUnitWithSSL(9998, 9999, certStoreConfig);

        assertEquals("clientKeystorePassword", kafkaUnitWithSSL.getClientKeystorePassword());
        assertEquals("clientTruststorePassword", kafkaUnitWithSSL.getClientTruststorePassword());
        assertEquals("/path/to/certs/client.keystore.jks", kafkaUnitWithSSL.getClientKeystorePath());
        assertEquals("/path/to/certs/server.keystore.jks", kafkaUnitWithSSL.getClientTruststorePath());

    }
}
