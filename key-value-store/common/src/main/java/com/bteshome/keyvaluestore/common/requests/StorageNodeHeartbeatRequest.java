package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Getter
@Setter
public class StorageNodeHeartbeatRequest implements Serializable, Message {
    private String id;
    private String metadataVersion;

    public StorageNodeHeartbeatRequest(Map<String, String> nodeInfo, String metadataVersion) {
        this.id = Validator.notEmpty(nodeInfo.get("id"));
        this.metadataVersion = Validator.notEmpty(metadataVersion);
    }

    @Override
    public ByteString getContent() {
        final String message = "STORAGE_NODE_HEARTBEAT " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}