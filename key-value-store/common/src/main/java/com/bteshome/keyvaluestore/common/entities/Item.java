package com.bteshome.keyvaluestore.common.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Item {
    private String key;
    private String value;
    
    @Override
    public String toString() {
        return key + "=" + value;
    }
}
