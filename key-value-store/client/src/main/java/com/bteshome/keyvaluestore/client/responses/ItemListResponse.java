package com.bteshome.keyvaluestore.client.responses;

import com.bteshome.keyvaluestore.common.Tuple;
import com.bteshome.keyvaluestore.common.Tuple3;
import lombok.*;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemListResponse {
    private int httpStatusCode;
    private String errorMessage;
    private String leaderEndpoint;
    private List<ItemResponse<byte[]>> items;
    private CursorPosition cursorPosition;
}