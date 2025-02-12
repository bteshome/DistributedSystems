package com.bteshome.keyvaluestore.client.requests;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemGetRequest {
    private String table;
    private int partition;
    private String key;
    private IsolationLevel isolationLevel;
}