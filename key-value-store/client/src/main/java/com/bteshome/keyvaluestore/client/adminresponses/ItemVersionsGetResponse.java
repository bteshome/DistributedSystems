package com.bteshome.keyvaluestore.client.adminresponses;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.*;

import java.util.HashMap;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemVersionsGetResponse {
    private int httpStatusCode;
    private String errorMessage;
    private String leaderEndpoint;
    private List<byte[]> valueVersions;
}