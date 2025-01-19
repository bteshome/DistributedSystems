package com.bteshome.keyvaluestore.client.responses;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemPutResponse {
    private int httpStatus;
    private String errorMessage;
    private String leaderEndpoint;
}