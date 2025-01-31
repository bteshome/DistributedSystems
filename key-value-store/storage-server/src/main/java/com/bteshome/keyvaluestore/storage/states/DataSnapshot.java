package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

@Setter
@Getter
public class DataSnapshot implements Serializable {
    private LogPosition lastCommittedOffset;
    private ConcurrentHashMap<ItemKey, String> data;
}
