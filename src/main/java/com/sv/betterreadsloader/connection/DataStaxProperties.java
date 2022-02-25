package com.sv.betterreadsloader.connection;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.io.File;

@ConfigurationProperties(prefix = "datastax.astra")
public class DataStaxProperties {

    private File secureConnectBundle;

    public DataStaxProperties() {
    }

    public File getSecureConnectBundle() {
        return secureConnectBundle;
    }

    public void setSecureConnectBundle(File secureConnectBundle) {
        this.secureConnectBundle = secureConnectBundle;
    }
}
