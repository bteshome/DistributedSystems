package com.bteshome.keyvaluestore.storage.responses;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WALGetReplicaEndOffsetResponse {
    private int httpStatusCode;
    private String errorMessage;
    private LogPosition endOffset;
}
