package com.bteshome.keyvaluestore.client.responses;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.*;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemCountAndOffsetsResponse {
    private int httpStatusCode;
    private String errorMessage;
    private String leaderEndpoint;
    private int count;
    private LogPosition commitedOffset;
    private Map<String, LogPosition> replicaEndOffsets;
}