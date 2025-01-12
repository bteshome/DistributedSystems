package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;

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