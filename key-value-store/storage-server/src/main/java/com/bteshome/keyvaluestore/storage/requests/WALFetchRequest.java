package com.bteshome.keyvaluestore.storage.requests;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WALFetchRequest {
    private String id;
    private String table;
    private int partition;
    private LogPosition lastFetchOffset;
    private int maxNumRecords;

    public WALFetchRequest() {}

    public WALFetchRequest(
            String nodeId,
            String table,
            int partition,
            LogPosition lastFetchOffset,
            int maxNumRecords) {
        this.id = Validator.notEmpty(nodeId);
        this.table = table;
        this.partition = partition;
        this.lastFetchOffset = lastFetchOffset;
        this.maxNumRecords = maxNumRecords;
    }
}