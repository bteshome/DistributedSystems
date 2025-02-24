package com.bteshome.keyvaluestore.client.requests;

import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemPutRequest {
    private String table;
    private int partition;
    private List<Item> items = new ArrayList<>();
    private AckType Ack;
    boolean withVersionCheck;
}