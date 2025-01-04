package com.bteshome.keyvaluestore.metadata.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.common.base.Strings;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class NotFoundResponse implements Message {
    @Override
    public ByteString getContent() {
        final String message = "NOT_FOUND";
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}