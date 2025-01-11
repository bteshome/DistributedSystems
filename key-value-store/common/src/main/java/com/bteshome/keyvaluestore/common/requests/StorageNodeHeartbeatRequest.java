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
public class StorageNodeHeartbeatRequest {
    private String id;
    private long lastFetchedMetadataVersion;

    public StorageNodeHeartbeatRequest() {}

    public StorageNodeHeartbeatRequest(
            Map<String, String> nodeInfo,
            long lastFetchedMetadataVersion) {
        this.id = Validator.notEmpty(nodeInfo.get("id"));
        this.lastFetchedMetadataVersion = lastFetchedMetadataVersion;
    }
}