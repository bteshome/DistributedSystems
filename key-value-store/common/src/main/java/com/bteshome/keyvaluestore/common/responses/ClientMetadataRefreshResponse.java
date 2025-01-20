package com.bteshome.keyvaluestore.common.responses;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
@NoArgsConstructor
@AllArgsConstructor
public class ClientMetadataRefreshResponse implements Serializable, Message {
    private Map<EntityType, Map<String, Object>> state;
    private boolean modified;
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public ByteString getContent() {
        final String message = "200 " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}