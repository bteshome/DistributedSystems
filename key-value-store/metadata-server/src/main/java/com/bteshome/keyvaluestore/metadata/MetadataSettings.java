package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.MetadataClientSettings;
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
    private MetadataClientSettings.PeerInfo node;
    @Value("${server.port}")
    private int restPort;
    private List<MetadataClientSettings.PeerInfo> peers;
    private long storageNodeHeartbeatMonitorIntervalMs;
    private long storageNodeHeartbeatExpectIntervalMs;
    private long storageNodeHeartbeatSendIntervalMs;
    private long storageNodeMetadataRefreshIntervalMs;
    private long replicaMonitorIntervalMs;
    private long replicaLagThresholdRecords;
    private long replicaLagThresholdTimeMs;
    private long replicaFetchIntervalMs;
    private int replicaFetchMaxNumRecords;
    private long snapshotIntervalMs;
    private int numPartitionsDefault;
    private int numPartitionsMax;
    private int replicationFactorDefault;
    private int minInSyncReplicasDefault;
    private int ringNumVirtualPartitions;
    private int writeBatchSizeMax;
    private long expirationMonitorIntervalMs;
    private UUID localClientId;
}