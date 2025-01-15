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
            String nodeId,
            long lastFetchedMetadataVersion) {
        this.id = Validator.notEmpty(nodeId);
        this.lastFetchedMetadataVersion = lastFetchedMetadataVersion;
    }
}