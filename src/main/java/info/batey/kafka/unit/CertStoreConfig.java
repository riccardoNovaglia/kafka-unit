package info.batey.kafka.unit;

public class CertStoreConfig {
    private final String certStoreDirectory;
    private final String clientKeystorePassword;
    private final String clientTruststorePassword;
    private final String serverKeystorePassword;
    private final String serverTruststorePassword;

    public CertStoreConfig(
        String certStoreDirectory, String clientKeystorePassword, String clientTruststorePassword,
        String serverKeystorePassword,
        String serverTruststorePassword
    ) {
        this.certStoreDirectory = certStoreDirectory;
        this.clientKeystorePassword = clientKeystorePassword;
        this.clientTruststorePassword = clientTruststorePassword;
        this.serverKeystorePassword = serverKeystorePassword;
        this.serverTruststorePassword = serverTruststorePassword;
    }

    public String getCertStoreDirectory() {
        return certStoreDirectory;
    }

    public String getClientKeystorePassword() {
        return clientKeystorePassword;
    }

    public String getClientTruststorePassword() {
        return clientTruststorePassword;
    }

    public String getServerKeystorePassword() {
        return serverKeystorePassword;
    }

    public String getServerTruststorePassword() {
        return serverTruststorePassword;
    }
}
