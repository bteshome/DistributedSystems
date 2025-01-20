package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.JavaSerDe;
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

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ClientMetadataRefreshRequest implements Serializable, Message {
    private long lastFetchedVersion;
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public ByteString getContent() {
        final String message = MetadataRequestType.CLIENT_METADATA_REFRESH + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}
