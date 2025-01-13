package com.bteshome.keyvaluestore.common.requests;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DataGetRequest {
    String key;
    private String table;
    private int partition;

    public DataGetRequest() {}

    public DataGetRequest(
            String key,
            String table,
            int partition) {
        this.key = key;
        this.table = table;
        this.partition = partition;
    }
}