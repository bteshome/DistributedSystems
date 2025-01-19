package com.bteshome.keyvaluestore.client.responses;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemPutResponse {
    private int httpStatusCode;
    private String errorMessage;
    private String leaderEndpoint;
}