package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ConfigurationLoader {
    public static void load(Map<EntityType, Map<String, Object>> state, MetadataSettings metadataSettings) {
        log.info("Loading configuration ...");

        state.get(EntityType.CONFIGURATION).put(ConfigKeys.STORAGE_NODE_HEARTBEAT_MONITOR_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getStorageNodeHeartbeatMonitorIntervalMs(), 10000L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.STORAGE_NODE_HEARTBEAT_EXPECT_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getStorageNodeHeartbeatExpectIntervalMs(), 10000L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.STORAGE_NODE_HEARTBEAT_SEND_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getStorageNodeHeartbeatSendIntervalMs(), 15000L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.STORAGE_NODE_METADATA_REFRESH_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getStorageNodeMetadataRefreshIntervalMs(), 30000L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.WAL_FLUSH_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getWalFlushIntervalMs(), 200L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.REPLICA_MONITOR_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getReplicaMonitorIntervalMs(), 500L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.REPLICA_LAG_THRESHOLD_RECORDS_KEY,
                Validator.setDefault(metadataSettings.getReplicaLagThresholdRecords(), 4000L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.REPLICA_LAG_THRESHOLD_TIME_MS_KEY,
                Validator.setDefault(metadataSettings.getReplicaLagThresholdTimeMs(), 10000L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.REPLICA_FETCH_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getReplicaFetchIntervalMs(), 500L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.REPLICA_FETCH_MAX_NUM_RECORDS_KEY,
                Validator.setDefault(metadataSettings.getReplicaFetchMaxNumRecords(), 10000));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.DATA_SNAPSHOT_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getDataSnapshotIntervalMs(), 300000L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.END_OFFSET_SNAPSHOT_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getEndOffsetSnapshotIntervalMs(), 10000L));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.NUM_PARTITIONS_DEFAULT_KEY,
                Validator.setDefault(metadataSettings.getNumPartitionsDefault(), 1));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.NUM_PARTITIONS_MAX_KEY,
                Validator.setDefault(metadataSettings.getNumPartitionsMax(), 8));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.REPLICATION_FACTOR_DEFAULT_KEY,
                Validator.setDefault(metadataSettings.getReplicationFactorDefault(), 1));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.MIN_IN_SYNC_REPLICAS_DEFAULT,
                Validator.setDefault(metadataSettings.getMinInSyncReplicasDefault(), 1));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.RING_NUM_VIRTUAL_PARTITIONS_KEY,
                Validator.setDefault(metadataSettings.getRingNumVirtualPartitions(), 3));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.WRITE_BATCH_SIZE_MAX_KEY,
                Validator.setDefault(metadataSettings.getWriteBatchSizeMax(), 100));
        state.get(EntityType.CONFIGURATION).put(ConfigKeys.EXPIRATION_MONITOR_INTERVAL_MS_KEY,
                Validator.setDefault(metadataSettings.getExpirationMonitorIntervalMs(), 10000L));

        log.info("Configuration loaded.");
    }
}
