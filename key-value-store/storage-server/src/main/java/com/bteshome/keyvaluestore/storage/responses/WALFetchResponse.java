package com.bteshome.keyvaluestore.storage.responses;

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
    private List<String> entries;
    private Map<String, Long> replicaEndOffsets;
    private long commitedOffset;
}
