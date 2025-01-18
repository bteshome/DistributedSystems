package com.bteshome.keyvaluestore.client;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemGetResponse {
    private int httpStatus;
    private String errorMessage;
    private String leaderEndpoint;
    private String value;
}