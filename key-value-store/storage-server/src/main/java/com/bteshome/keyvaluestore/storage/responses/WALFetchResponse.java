package com.bteshome.keyvaluestore.storage.responses;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.storage.states.WALEntry;
import lombok.*;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WALFetchResponse {
    private int httpStatusCode;
    private String errorMessage;
    private List<WALEntry> entries;
    private Map<String, LogPosition> replicaEndOffsets;
    private LogPosition commitedOffset;
    private LogPosition truncateToOffset;
}
