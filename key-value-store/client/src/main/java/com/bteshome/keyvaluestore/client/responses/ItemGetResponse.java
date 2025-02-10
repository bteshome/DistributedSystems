package com.bteshome.keyvaluestore.client.responses;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemGetResponse {
    private int httpStatusCode;
    private String errorMessage;
    private String leaderEndpoint;
    private byte[] value;
}