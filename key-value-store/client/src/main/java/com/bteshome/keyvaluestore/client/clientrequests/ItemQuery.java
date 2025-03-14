package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import com.bteshome.keyvaluestore.client.responses.CursorPosition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemQuery {
    private String table;
    private String partitionKey;
    private String indexName;
    private String indexKey;
    private Map<Integer, CursorPosition> cursorPositions = new HashMap<>();
    private int limit;
    private IsolationLevel isolationLevel;
}