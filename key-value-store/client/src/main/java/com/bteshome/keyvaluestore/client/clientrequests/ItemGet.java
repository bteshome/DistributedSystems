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
public class ItemGet {
    private String table;
    private String partitionKey;
    private String key;
    private IsolationLevel isolationLevel;
}