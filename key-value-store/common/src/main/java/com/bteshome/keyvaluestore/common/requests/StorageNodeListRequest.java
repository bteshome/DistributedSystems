package com.bteshome.keyvaluestore.common.requests;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class StorageNodeListRequest implements Serializable, Message {
    @Override
    public ByteString getContent() {
        final String message = "STORAGE_NODE_LIST";
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}