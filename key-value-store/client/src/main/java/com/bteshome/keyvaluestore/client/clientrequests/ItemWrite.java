package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemWrite<T> {
    private String table;
    private String key;
    private T value;
    private AckType ack;
    private int maxRetries;
}