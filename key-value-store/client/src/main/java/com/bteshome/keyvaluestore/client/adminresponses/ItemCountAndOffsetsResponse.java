package com.bteshome.keyvaluestore.client.adminresponses;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemCountAndOffsetsResponse {
    private int httpStatusCode;
    private String errorMessage;
    private int count;
    private LogPosition commitedOffset;
    private LogPosition endOffset;
}