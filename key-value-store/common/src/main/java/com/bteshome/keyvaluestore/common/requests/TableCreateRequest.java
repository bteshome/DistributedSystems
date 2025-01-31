package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

@Getter
@Setter
public class TableCreateRequest implements Serializable, Message {
    private String tableName;
    private int numPartitions;
    private int replicationFactor;
    private int minInSyncReplicas;
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

    @Override
    public ByteString getContent() {
        final String message = MetadataRequestType.TABLE_CREATE + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}