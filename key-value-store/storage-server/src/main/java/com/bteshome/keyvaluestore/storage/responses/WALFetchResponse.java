package com.bteshome.keyvaluestore.storage.responses;

import com.bteshome.keyvaluestore.storage.WAL;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
public class WALFetchResponse {
    private HttpStatus httpStatus;
    private String errorMessage;
    private List<String> entries;
    private Map<String, Long> replicaEndOffsets;
    private long commitedOffset;

    public WALFetchResponse(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public WALFetchResponse(HttpStatus httpStatus, String errorMessage) {
        this.httpStatus = httpStatus;
        this.errorMessage = errorMessage;
    }

    public WALFetchResponse(List<String> entries, Map<String, Long> replicaEndOffsets, long commitedOffset) {
        this.httpStatus = HttpStatus.OK;
        this.entries = entries;
        this.replicaEndOffsets = replicaEndOffsets;
        this.commitedOffset = commitedOffset;
    }
}
