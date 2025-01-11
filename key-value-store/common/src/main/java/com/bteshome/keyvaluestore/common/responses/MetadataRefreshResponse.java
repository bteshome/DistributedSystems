package com.bteshome.keyvaluestore.common.responses;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Getter
@Setter
public class MetadataRefreshResponse implements Serializable, Message {
    private Map<EntityType, Map<String, Object>> state;
    private String heartbeatEndpoint;
    private boolean modified;
    @Serial
    private static final long serialVersionUID = 1L;

    public MetadataRefreshResponse(
            Map<EntityType, Map<String, Object>> state,
            String heartbeatEndpoint) {
        this.state = state;
        this.heartbeatEndpoint = heartbeatEndpoint;
        this.modified = true;
    }

    public MetadataRefreshResponse(
            String heartbeatEndpoint) {
        this.heartbeatEndpoint = heartbeatEndpoint;
        this.modified = false;
    }

    @Override
    public ByteString getContent() {
        final String message = "200 " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}