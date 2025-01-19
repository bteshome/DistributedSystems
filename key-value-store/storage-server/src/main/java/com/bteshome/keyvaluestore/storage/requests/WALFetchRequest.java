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
    private long lastFetchedEndOffset;
    private int lastFetchedLeaderTerm;
    private int maxNumRecords;

    public WALFetchRequest() {}

    public WALFetchRequest(
            String nodeId,
            String table,
            int partition,
            long lastFetchedEndOffset,
            int lastFetchedLeaderTerm,
            int maxNumRecords) {
        this.id = Validator.notEmpty(nodeId);
        this.table = table;
        this.partition = partition;
        this.lastFetchedEndOffset = lastFetchedEndOffset;
        this.lastFetchedLeaderTerm = lastFetchedLeaderTerm;
        this.maxNumRecords = maxNumRecords;
    }
}