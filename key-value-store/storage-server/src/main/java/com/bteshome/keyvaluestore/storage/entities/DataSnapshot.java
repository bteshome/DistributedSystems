package com.bteshome.keyvaluestore.storage.entities;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DataSnapshot implements Serializable {
    private LogPosition lastCommittedOffset;
    private HashMap<ItemKey, byte[]> data;
}
