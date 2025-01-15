package com.bteshome.keyvaluestore.storage.requests;

import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WALFetchAckRequest {
    private String id;
    private String table;
    private int partition;
    private long endOffset;

    public WALFetchAckRequest() {}

    public WALFetchAckRequest(
            String nodeId,
            String table,
            int partition,
            long endOffset) {
        this.id = Validator.notEmpty(nodeId);
        this.table = table;
        this.partition = partition;
        this.endOffset = endOffset;
    }
}