package com.bteshome.keyvaluestore.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemPutRequest {
    private String table;
    private String key;
    private String value;
}