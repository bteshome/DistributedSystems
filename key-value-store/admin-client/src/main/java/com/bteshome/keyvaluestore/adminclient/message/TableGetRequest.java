package com.bteshome.keyvaluestore.adminclient.message;

import com.bteshome.keyvaluestore.adminclient.common.Validator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class TableGetRequest implements Message {
    private final String tableName;

    public TableGetRequest(String tableName) {
        this.tableName = Validator.notEmpty(tableName);
    }

    @Override
    public ByteString getContent() {
        final String message = this.toString();
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }

    public String toString() {
        return "TABLE_GET name=%s".formatted(tableName);
    }
}