package com.bteshome.keyvaluestore.metadata.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class TableGetResponse implements Message {
    private final String tableName;
    private final int numPartitions;

    public TableGetResponse(String tableName, int numPartitions) {
        this.tableName = tableName;
        this.numPartitions = numPartitions;
    }

    @Override
    public ByteString getContent() {
        final String message = this.toString();
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }

    public String toString() {
        return "name=%s partitions=%s".formatted(tableName, numPartitions);
    }
}