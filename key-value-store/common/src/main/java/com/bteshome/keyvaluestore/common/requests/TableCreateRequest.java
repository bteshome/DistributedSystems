package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class TableCreateRequest implements Serializable, Message {
    private String tableName;
    private int numPartitions;
    private int replicationFactor;
    private int minInSyncReplicas;

    public void Validate() {
        int numPartitionsDefault = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.NUM_PARTITIONS_DEFAULT_KEY);
        int maxNumPartitions = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.NUM_PARTITIONS_MAX_KEY);
        int replicationFactorDefault = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICATION_FACTOR_DEFAULT_KEY);
        int minInSyncReplicasDefault = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.MIN_IN_SYNC_REPLICAS_DEFAULT);

        this.tableName = Validator.notEmpty(tableName);
        this.numPartitions = Validator.setDefault(numPartitions, numPartitionsDefault);
        Validator.notGreaterThan(numPartitions, maxNumPartitions);
        this.replicationFactor = Validator.setDefault(replicationFactor, replicationFactorDefault);
        this.minInSyncReplicas = Validator.setDefault(minInSyncReplicas, minInSyncReplicasDefault);
        Validator.notGreaterThan(minInSyncReplicas, replicationFactor);
    }

    @Override
    public ByteString getContent() {
        final String message = RequestType.TABLE_CREATE + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}