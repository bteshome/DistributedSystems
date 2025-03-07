package com.bteshome.keyvaluestore.common.entities;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Item {
    private String key;
    private String partitionKey;
    private byte[] value;
    private Map<String, String> indexKeys = new HashMap<>();
    private LogPosition previousVersion;
}
