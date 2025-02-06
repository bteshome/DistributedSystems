package com.bteshome.keyvaluestore.client.requests;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemDeleteRequest {
    private String table;
    private int partition;
    private AckType ack;
    private List<String> keys = new ArrayList<>();
}