package com.bteshome.keyvaluestore.client.responses;

import com.bteshome.keyvaluestore.common.entities.EntityType;
import com.bteshome.keyvaluestore.common.entities.Table;
import lombok.*;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClientMetadataFetchResponse {
    private int httpStatusCode;
    private String errorMessage;
    private String serializedMetadata;
}