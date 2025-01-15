package com.bteshome.keyvaluestore.storage.requests;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DataWriteRequest {
    private String table;
    private String key;
    private String value;
}