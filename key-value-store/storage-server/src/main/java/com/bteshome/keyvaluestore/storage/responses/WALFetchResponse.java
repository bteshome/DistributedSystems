package com.bteshome.keyvaluestore.storage.responses;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.storage.entities.WALEntry;
import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WALFetchResponse {
    private int httpStatusCode;
    private String errorMessage;
    private List<WALEntry> entries;
    private LogPosition commitedOffset;
    private LogPosition truncateToOffset;
    private byte[] dataSnapshotBytes;
    private WALFetchPayloadType payloadType;
}
