package com.bteshome.keyvaluestore.client.responses;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemDeleteResponse {
    private int httpStatusCode;
    private String errorMessage;
    private String leaderEndpoint;
    private LogPosition endOffset;
}