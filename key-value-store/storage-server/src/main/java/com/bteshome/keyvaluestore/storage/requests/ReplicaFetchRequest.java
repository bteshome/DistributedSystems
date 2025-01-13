package com.bteshome.keyvaluestore.storage.requests;

import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class ReplicaFetchRequest {
    private String id;
    private String table;
    private int partition;
    private long lastFetchedOffset;
    private int maxRecords = 100;

    public ReplicaFetchRequest() {}

    public ReplicaFetchRequest(
            Map<String, String> nodeInfo,
            String table,
            int partition,
            long lastFetchedOffset) {
        this.id = Validator.notEmpty(nodeInfo.get("id"));
        this.table = table;
        this.partition = partition;
        this.lastFetchedOffset = lastFetchedOffset;
    }
}