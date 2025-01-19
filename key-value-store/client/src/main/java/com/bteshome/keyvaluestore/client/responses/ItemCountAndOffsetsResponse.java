package com.bteshome.keyvaluestore.client.responses;

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
    private long commitedOffset;
    private Map<String, Long> replicaEndOffsets;
}