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
public class StorageNodeJoinRequest implements Serializable, Message {
    private String id;
    private String host;
    private int port;
    private int jmxPort;
    private String rack;
    private String storageDir;

    public StorageNodeJoinRequest(Map<String, String> nodeInfo) {
        this.id = Validator.notEmpty(nodeInfo.get("id"));
        this.host = Validator.notEmpty(nodeInfo.get("host"));
        this.port = Validator.inRange(Integer.parseInt(nodeInfo.get("port")), 0, 65535);
        this.jmxPort = Validator.inRange(Integer.parseInt(nodeInfo.get("jmx-port")), 0, 65535);
        this.rack = Validator.setDefault(nodeInfo.get("rack"), "NA");
        Validator.notEqual(this.getPort(), this.getJmxPort(), "Port and JMX port must be different.");
        storageDir = Validator.notEmpty(nodeInfo.get("storage-dir"));
    }

    @Override
    public ByteString getContent() {
        final String message = RequestType.STORAGE_NODE_JOIN + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}