package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class BatchWrite {
    private String table;
    private List<Item> items;
}