/*
package com.bteshome.keyvaluestore.adminclient.message;

import com.bteshome.keyvaluestore.adminclient.common.Validator;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.nio.charset.StandardCharsets;

public class TableListRequest implements Message {
    @Override
    public ByteString getContent() {
        final String message = this.toString();
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }

    public String toString() {
        return "TABLE_LIST";
    }
}*/
