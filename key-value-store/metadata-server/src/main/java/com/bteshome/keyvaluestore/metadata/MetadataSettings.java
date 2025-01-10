package com.bteshome.keyvaluestore.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
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
    private Map<String, String> node;
    private List<Map<String, String>> peers;
    private int numPartitionsMax;
    private int numPartitionsDefault;
    private int replicationFactorDefault;
    private long storageNodeHeartbeatMonitorIntervalMs;
    private long storageNodeHeartbeatExpectIntervalMs;
    private long storageNodeHeartbeatSendIntervalMs;
    private long storageNodeMetadataLagThreshold;
    private long storageNodeMetadataLagMs;
    private UUID localClientId;
}