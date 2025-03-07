package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.client.requests.AckType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class BatchWrite<T> {
    private String table;
    private final List<String> partitionKeys = new ArrayList<>();
    private final List<String> keys = new ArrayList<>();
    private final List<T> values = new ArrayList<>();
    private final List<Map<String, String>> indexKeys = new ArrayList<>();
    private AckType ack;
    private int maxRetries;
}