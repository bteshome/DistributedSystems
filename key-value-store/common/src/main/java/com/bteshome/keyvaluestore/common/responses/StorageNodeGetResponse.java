package com.bteshome.keyvaluestore.common.responses;

import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
@NoArgsConstructor
public class StorageNodeGetResponse implements Serializable, Message {
    private StorageNode storageNodeCopy;

    public StorageNodeGetResponse(StorageNode storageNodeCopy) {
        this.storageNodeCopy = storageNodeCopy;
    }

    @Override
    public ByteString getContent() {
        final String message = "200 " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}