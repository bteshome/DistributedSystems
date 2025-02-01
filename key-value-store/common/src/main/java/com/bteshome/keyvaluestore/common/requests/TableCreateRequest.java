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
    private Duration timeToLive;

    @Override
    public ByteString getContent() {
        final String message = MetadataRequestType.TABLE_CREATE + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}