package com.bteshome.keyvaluestore.storage;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@ConfigurationProperties
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AppSettings {
    @Value("${storage-server.client-id}")
    private UUID clientId;
    private String storageDir;
    private String peerAddress;
    @Value("${storage-server.node.id}")
    private String nodeId;
    @Value("${storage-server.node.host}")
    private String host;
    @Value("${storage-server.node.port}")
    private int port;
    @Value("${storage-server.node.jmxPort}")
    private int jmxPort;
    @Value("${storage-server.node.rack}")
    private String rack;
    @Value("${metadata-server.group-id}")
    private UUID groupId;
    @Value("${metadata-server.peers}")
    private List<Map<String, String>> peers;
}
