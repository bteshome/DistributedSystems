package com.bteshome.keyvaluestore.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
@ConfigurationProperties(prefix = "metadata")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MetadataSettings {
    private UUID groupId;
    private String storageDir;
    private NodeInfo node;
    @Value("${server.port}")
    private int restPort;
    private List<NodeInfo> peers;
    private int numPartitionsMax;
    private int numPartitionsDefault;
    private int replicationFactorDefault;
    private int minInSyncReplicasDefault;
    private long storageNodeHeartbeatMonitorIntervalMs;
    private long storageNodeHeartbeatExpectIntervalMs;
    private long storageNodeHeartbeatSendIntervalMs;
    private long storageNodeMetadataLagThreshold;
    private long storageNodeMetadataLagMs;
    private long storageNodeReplicaMonitorIntervalMs;
    private long storageNodeReplicaLagThreshold;
    private long storageNodeReplicaFetchIntervalMs;
    private int storageNodeRingNumVirtualNodes;
    private long writeTimeoutMs;
    private long walFetchIntervalMs;
    private UUID localClientId;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NodeInfo {
        private String id;
        private String host;
        private int port;
    }
}