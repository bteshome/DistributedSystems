package com.bteshome.keyvaluestore.client.responses;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemPutResponse {
    private int httpStatusCode;
    private String errorMessage;
    private String leaderEndpoint;
    private LogPosition endOffset;
}