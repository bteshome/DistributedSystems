package com.bteshome.keyvaluestore.storage.responses;

import lombok.*;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WALFetchResponse {
    private HttpStatus httpStatus;
    private String errorMessage;
    private List<String> entries;
    private Map<String, Long> replicaEndOffsets;
    private long commitedOffset;
}
