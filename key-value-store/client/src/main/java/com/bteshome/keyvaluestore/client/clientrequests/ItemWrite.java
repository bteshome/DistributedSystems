package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemWrite<T> {
    private String table;
    private String partitionKey;
    private String key;
    private T value;
    private Map<String, String> indexKeys;
    private AckType ack;
    private int maxRetries;
    private LogPosition previousVersion;
}