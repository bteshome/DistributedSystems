package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DataSnapshot implements Serializable {
    private LogPosition lastCommittedOffset;
    private HashMap<ItemKey, String> data;
}
