package com.bteshome.keyvaluestore.storage.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "storage")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StorageSettings {
    private NodeInfo node;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NodeInfo {
        private String id;
        private String host;
        private int port;
        private int jmxPort;
        private String rack;
        private String storageDir;
    }
}
