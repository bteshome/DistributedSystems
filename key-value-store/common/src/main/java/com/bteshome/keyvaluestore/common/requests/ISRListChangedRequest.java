package com.bteshome.keyvaluestore.common.requests;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ISRListChangedRequest {
    private final String tableName;
    private final int partitionId;
}