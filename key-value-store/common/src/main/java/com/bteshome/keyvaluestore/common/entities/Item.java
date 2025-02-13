package com.bteshome.keyvaluestore.common.entities;

import com.bteshome.keyvaluestore.common.LogPosition;
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
    private byte[] value;
    private LogPosition previousVersion;
}
