package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemQuery {
    private String table;
    private String indexName;
    private String indexKey;
    private IsolationLevel isolationLevel;
}