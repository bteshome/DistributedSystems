package com.bteshome.keyvaluestore.storage.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "storage")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class StorageSettings {
    private NodeInfo node;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Slf4j
    public static class NodeInfo {
        private String id;
        private String host;
        private int port;
        private int managementPort;
        private String rack;
        private String storageDir;


        public void print() {
            log.info("StorageSettings: id={}", id);
            log.info("StorageSettings: host={}", host);
            log.info("StorageSettings: port={}", port);
            log.info("StorageSettings: managementPort={}", managementPort);
            log.info("StorageSettings: rack={}", rack);
            log.info("StorageSettings: storageDir={}", storageDir);
        }
    }

    public void print() {
        if (node != null)
            node.print();
        else
            log.info("StorageSettings: node=null");
    }
}
