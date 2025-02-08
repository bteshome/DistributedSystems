package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.client.requests.AckType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemDelete {
    private String table;
    private String key;
    private AckType ack;
    private int retries;
}