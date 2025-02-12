package com.bteshome.keyvaluestore.client.adminrequests;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemVersionsGetRequest {
    private String table;
    private int partition;
    private String key;
}