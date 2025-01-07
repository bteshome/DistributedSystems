package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.MetadataSettings;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.common.JavaSerDe;
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

    public void Validate(MetadataSettings metadataSettings) {
        this.tableName = Validator.notEmpty(tableName);
        this.numPartitions = Validator.setDefault(numPartitions, metadataSettings.getNumPartitionsDefault());
        Validator.inRange(numPartitions,
                metadataSettings.getNumPartitionsMin(),
                metadataSettings.getNumPartitionsMax());
        this.replicationFactor = Validator.setDefault(replicationFactor, metadataSettings.getReplicationFactorDefault());
    }

    @Override
    public ByteString getContent() {
        final String message = "TABLE_CREATE " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}