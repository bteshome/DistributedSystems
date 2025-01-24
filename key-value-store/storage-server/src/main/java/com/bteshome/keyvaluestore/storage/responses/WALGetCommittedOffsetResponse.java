package com.bteshome.keyvaluestore.storage.responses;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.*;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WALGetCommittedOffsetResponse {
    private int httpStatusCode;
    private String errorMessage;
    private LogPosition committedOffset;
}
