package com.bteshome.keyvaluestore.client.requests;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemQueryRequest {
    private String table;
    private String lastReadItemKey;
    private int partition;
    private String indexName;
    private String indexKey;
    private int limit;
    private IsolationLevel isolationLevel;
}