package com.bteshome.keyvaluestore.storage.requests;

import com.bteshome.keyvaluestore.common.Validator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class WALGetCommittedOffsetRequest {
    private String table;
    private int partition;
}