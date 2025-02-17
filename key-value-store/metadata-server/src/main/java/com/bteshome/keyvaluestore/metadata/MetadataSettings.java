package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.PeerInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
@ConfigurationProperties(prefix = "metadata")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class MetadataSettings {
    private UUID groupId;
    private String storageDir;
    private PeerInfo node;
    private List<PeerInfo> peers;
    private long storageNodeHeartbeatMonitorIntervalMs;
    private long storageNodeHeartbeatExpectIntervalMs;
    private long storageNodeHeartbeatSendIntervalMs;
    private long storageNodeMetadataRefreshIntervalMs;
    private long replicaMonitorIntervalMs;
    private long replicaLagThresholdRecords;
    private long replicaLagThresholdTimeMs;
    private long replicaFetchIntervalMs;
    private int replicaFetchMaxNumRecords;
    private long dataSnapshotIntervalMs;
    private long endOffsetSnapshotIntervalMs;
    private int numPartitionsDefault;
    private int numPartitionsMax;
    private int replicationFactorDefault;
    private int minInSyncReplicasDefault;
    private int ringNumVirtualPartitions;
    private int writeBatchSizeMax;
    private long writeTimeoutMs;
    private long expirationMonitorIntervalMs;

    @Autowired
    private ConfigurableEnvironment environment;

    public int getRestPort() {
        String port = environment.getProperty("local.server.port");
        if (port == null)
            throw new RuntimeException("Failed to extract server port.");
        return Integer.parseInt(port);
    }

    public void print() {
        log.info("MetadataSettings: groupId={}", groupId);
        log.info("MetadataSettings: storageDir={}", groupId);
        log.info("MetadataSettings: node={}", node);
        log.info("MetadataSettings: storageNodeHeartbeatMonitorIntervalMs={}", storageNodeHeartbeatMonitorIntervalMs);
        log.info("MetadataSettings: storageNodeHeartbeatExpectIntervalMs={}", storageNodeHeartbeatExpectIntervalMs);
        log.info("MetadataSettings: storageNodeHeartbeatSendIntervalMs={}", storageNodeHeartbeatSendIntervalMs);
        log.info("MetadataSettings: storageNodeMetadataRefreshIntervalMs={}", storageNodeMetadataRefreshIntervalMs);
        log.info("MetadataSettings: replicaMonitorIntervalMs={}", replicaMonitorIntervalMs);
        log.info("MetadataSettings: replicaLagThresholdRecords={}", replicaLagThresholdRecords);
        log.info("MetadataSettings: replicaLagThresholdTimeMs={}", replicaLagThresholdTimeMs);
        log.info("MetadataSettings: replicaFetchIntervalMs={}", replicaFetchIntervalMs);
        log.info("MetadataSettings: replicaFetchMaxNumRecords={}", replicaFetchMaxNumRecords);
        log.info("MetadataSettings: dataSnapshotIntervalMs={}", dataSnapshotIntervalMs);
        log.info("MetadataSettings: endOffsetSnapshotIntervalMs={}", endOffsetSnapshotIntervalMs);
        log.info("MetadataSettings: numPartitionsDefault={}", numPartitionsDefault);
        log.info("MetadataSettings: numPartitionsMax={}", numPartitionsMax);
        log.info("MetadataSettings: replicationFactorDefault={}", replicationFactorDefault);
        log.info("MetadataSettings: minInSyncReplicasDefault={}", minInSyncReplicasDefault);
        log.info("MetadataSettings: ringNumVirtualPartitions={}", ringNumVirtualPartitions);
        log.info("MetadataSettings: writeBatchSizeMax={}", writeBatchSizeMax);
        log.info("MetadataSettings: writeTimeoutMs={}", writeTimeoutMs);
        log.info("MetadataSettings: expirationMonitorIntervalMs={}", expirationMonitorIntervalMs);
    }
}