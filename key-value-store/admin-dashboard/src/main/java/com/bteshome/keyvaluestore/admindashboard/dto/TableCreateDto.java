package com.bteshome.keyvaluestore.admindashboard.dto;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.util.Map;

@Getter
@Setter
public class TableCreateDto {
    private String tableName;
    private String indexNames;
    private int numPartitions;
    private int replicationFactor;
    private int minInSyncReplicas;
    private boolean timeToLiveEnabled;
    private Duration timeToLive = Duration.ZERO;

    public void validate(Map<String, Object> configurations) {
        int numPartitionsDefault = (Integer)configurations.get(ConfigKeys.NUM_PARTITIONS_DEFAULT_KEY);
        int maxNumPartitions = (Integer)configurations.get(ConfigKeys.NUM_PARTITIONS_MAX_KEY);
        int replicationFactorDefault = (Integer)configurations.get(ConfigKeys.REPLICATION_FACTOR_DEFAULT_KEY);
        int minInSyncReplicasDefault = (Integer)configurations.get(ConfigKeys.MIN_IN_SYNC_REPLICAS_DEFAULT);

        this.tableName = Validator.notEmpty(tableName, "Table name");
        this.numPartitions = Validator.setDefault(numPartitions, numPartitionsDefault);
        Validator.notGreaterThan(numPartitions, maxNumPartitions, "Number of partitions");
        this.replicationFactor = Validator.setDefault(replicationFactor, replicationFactorDefault);
        this.minInSyncReplicas = Validator.setDefault(minInSyncReplicas, minInSyncReplicasDefault);
        Validator.notGreaterThan(minInSyncReplicas, replicationFactor, "Min in sync replicas", "replication factor");
    }
}
