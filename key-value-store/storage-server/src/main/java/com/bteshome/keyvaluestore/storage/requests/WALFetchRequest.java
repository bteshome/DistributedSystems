package com.bteshome.keyvaluestore.storage.requests;

import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WALFetchRequest {
    private String id;
    private String table;
    private int partition;
    private long lastFetchedOffset;
    private int maxNumRecords;

    public WALFetchRequest() {}

    public WALFetchRequest(
            String nodeId,
            String table,
            int partition,
            long lastFetchedOffset,
            int maxNumRecords) {
        this.id = Validator.notEmpty(nodeId);
        this.table = table;
        this.partition = partition;
        this.lastFetchedOffset = lastFetchedOffset;
        this.maxNumRecords = maxNumRecords;
    }
}