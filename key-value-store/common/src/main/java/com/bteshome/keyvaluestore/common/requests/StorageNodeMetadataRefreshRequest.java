package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class StorageNodeMetadataRefreshRequest implements Serializable, Message {
    private long lastFetchedVersion;
    private String id;
    @Serial
    private static final long serialVersionUID = 1L;

    public StorageNodeMetadataRefreshRequest(String id, long lastFetchedVersion) {
        this.id = Validator.notEmpty(id);
        this.lastFetchedVersion = lastFetchedVersion;
    }

    @Override
    public ByteString getContent() {
        final String message = MetadataRequestType.STORAGE_NODE_METADATA_REFRESH + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}


