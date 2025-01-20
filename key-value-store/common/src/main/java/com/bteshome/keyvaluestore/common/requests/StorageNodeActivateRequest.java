package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class StorageNodeActivateRequest implements Serializable, Message {
    private final String id;
    @Serial
    private static final long serialVersionUID = 1L;

    public StorageNodeActivateRequest(String id) {
        this.id = id;
    }

    @Override
    public ByteString getContent() {
        final String message = MetadataRequestType.STORAGE_NODE_ACTIVATE + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}