package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Getter
@Setter
public class StorageNodeLeaveRequest implements Serializable, Message {
    private String id;

    public void validate() {
        this.id = Validator.notEmpty(id);
    }

    @Override
    public ByteString getContent() {
        final String message = MetadataRequestType.STORAGE_NODE_LEAVE + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}