package com.bteshome.keyvaluestore.common;

public class ConfigKeys {
    public static final String NUM_PARTITIONS_MAX_KEY = "num-partitions-max";
    public static final String NUM_PARTITIONS_DEFAULT_KEY = "num-partitions-default";
    public static final String REPLICATION_FACTOR_DEFAULT_KEY = "replication-factor-default";
    public static final String MIN_IN_SYNC_REPLICAS_DEFAULT = "min-in-sync-replicas-default";
    public static final String STORAGE_NODE_HEARTBEAT_MONITOR_INTERVAL_MS_KEY = "storage-node-heartbeat-monitor-interval-ms";
    public static final String STORAGE_NODE_HEARTBEAT_EXPECT_INTERVAL_MS_KEY = "storage-node-heartbeat-expect-interval-ms";
    public static final String STORAGE_NODE_HEARTBEAT_SEND_INTERVAL_MS_KEY = "storage-node-heartbeat-send-interval-ms";
    public static final String STORAGE_NODE_METADATA_REFRESH_INTERVAL_MS_KEY = "storage-node-metadata-refresh-interval-ms";
    public static final String REPLICA_MONITOR_INTERVAL_MS_KEY = "replica-monitor-interval-ms";
    public static final String REPLICA_MONITOR_THREAD_POOL_SIZE_KEY = "replica-monitor-thread-pool-size";
    public static final String REPLICA_LAG_THRESHOLD_RECORDS_KEY = "replica-lag-threshold-records";
    public static final String REPLICA_LAG_THRESHOLD_TIME_MS_KEY = "replica-lag-threshold-time-ms";
    public static final String REPLICA_FETCH_INTERVAL_MS_KEY = "replica-fetch-interval-ms";
    public static final String REPLICA_FETCH_MAX_NUM_RECORDS_KEY = "replica-fetch-max-num-records";
    public static final String REPLICA_FETCH_THREAD_POOL_SIZE_KEY = "replica-fetch-thread-pool-size";
    public static final String DATA_SNAPSHOT_INTERVAL_MS_KEY = "data-snapshot-interval-ms";
    public static final String END_OFFSET_SNAPSHOT_INTERVAL_MS_KEY = "end-offset-snapshot-interval-ms";
    public static final String RING_NUM_VIRTUAL_PARTITIONS_KEY = "ring-num-virtual-partitions";
    public static final String WRITE_BATCH_SIZE_MAX_KEY = "write-batch-size-max";
    public static final String EXPIRATION_MONITOR_INTERVAL_MS_KEY = "expiration-monitor-interval-ms";
}
