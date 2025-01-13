package com.bteshome.keyvaluestore.storage.requests;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DataWriteRequest {
    private String table;
    private String key;
    private String value;

    public DataWriteRequest() {}

    public DataWriteRequest(String table, String key, String value) {
        this.table = table;
        this.key = key;
        this.value = value;
    }
}