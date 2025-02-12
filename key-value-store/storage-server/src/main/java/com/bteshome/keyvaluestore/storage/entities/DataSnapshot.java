package com.bteshome.keyvaluestore.storage.entities;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DataSnapshot implements Serializable {
    private LogPosition lastCommittedOffset;
    private HashMap<String, List<ItemValueVersion>> data;
}
