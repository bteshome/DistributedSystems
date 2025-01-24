package com.bteshome.keyvaluestore.common.responses;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import lombok.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StorageNodeHeartbeatResponse {
    private boolean laggingOnMetadata;
    private int httpStatusCode;
    private String errorMessage;
}