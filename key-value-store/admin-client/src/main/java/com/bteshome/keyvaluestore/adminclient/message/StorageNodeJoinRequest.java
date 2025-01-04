package com.bteshome.keyvaluestore.adminclient.message;

import com.bteshome.keyvaluestore.adminclient.common.Validator;
import com.bteshome.keyvaluestore.adminclient.dto.Node;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.common.base.Strings;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;

import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class StorageNodeJoinRequest implements Message {
    private String id;
    private String host;
    private int port;
    private int jmxPort;
    private String rack;
    //private String entity;
    //private long logEndOffset;
    //private int partitionCount;

    public StorageNodeJoinRequest(Node node) {
        this.id = Validator.notEmpty(node.getId());
        this.host = Validator.notEmpty(node.getHost());
        this.port = Validator.inRange(node.getPort(), 0, 65535);
        this.jmxPort = Validator.inRange(node.getJmxPort(), 0, 65535);
        this.rack = Strings.isNullOrEmpty(node.getRack()) ? "NA" : node.getRack().trim();
        Validator.notEqual(this.getPort(), this.getJmxPort(), "Port and JMX port must be different.");
    }

    @Override
    public ByteString getContent() {
        final String message = this.toString();
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }

    public String toString() {
        return "STORAGE_NODE_JOIN id=%s host=%s port=%s jmxPort=%s rack=%s".formatted(
                id, host, port, jmxPort, rack
        );
    }
}