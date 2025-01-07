package com.bteshome.keyvaluestore.common.requests;

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
public class StorageNodeGetRequest implements Serializable, Message {
    private final String id;

    public StorageNodeGetRequest(String id) {
        this.id = Validator.notEmpty(id);
    }

    @Override
    public ByteString getContent() {
        final String message = "STORAGE_NODE_GET " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}