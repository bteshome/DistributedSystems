package com.bteshome.keyvaluestore.adminclient.message;

import com.bteshome.keyvaluestore.adminclient.common.Validator;
import com.bteshome.keyvaluestore.adminclient.dto.Table;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class TableCreateRequest implements Message {
    private String tableName;
    private int numPartitions;

    public TableCreateRequest(Table table) {
        this.tableName = Validator.notEmpty(table.getName());
        this.numPartitions = Validator.positive(table.getNumPartitions());
    }

    @Override
    public ByteString getContent() {
        final String message = this.toString();
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }

    public String toString() {
        return "TABLE_CREATE name=%s partitions=%s".formatted(tableName, numPartitions);
    }
}