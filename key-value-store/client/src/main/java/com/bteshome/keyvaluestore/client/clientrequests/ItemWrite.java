package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemWrite {
    private String table;
    private Item item;
}