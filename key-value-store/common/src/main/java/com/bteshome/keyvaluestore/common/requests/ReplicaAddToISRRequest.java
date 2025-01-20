package com.bteshome.keyvaluestore.common.requests;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.entities.Replica;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Set;

@Getter
@Setter
public class ReplicaAddToISRRequest implements Serializable, Message {
    private final Set<Replica> replicas;

    @Serial
    private static final long serialVersionUID = 1L;

    public ReplicaAddToISRRequest(Set<Replica> replicas) {
        this.replicas = replicas;
    }

    @Override
    public ByteString getContent() {
        final String message = MetadataRequestType.REPLICA_ADD_TO_ISR + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}