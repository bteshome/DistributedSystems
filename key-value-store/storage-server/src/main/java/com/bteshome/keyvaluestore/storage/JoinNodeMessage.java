package com.bteshome.keyvaluestore.storage;

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
@Builder
public class JoinNodeMessage implements Message {
    private String id;
    private String host;
    private int port;
    private int jmxPort;
    private String rack;

    public JoinNodeMessage(
            String id,
            String host,
            int port,
            int jmxPort,
            String rack) {
        this.id = Validator.notEmpty(id);
        this.host = Validator.notEmpty(host);
        this.port = Validator.inRange(port, 0, 65535);
        this.jmxPort = Validator.inRange(jmxPort, 0, 65535);
        Validator.notEqual(this.getPort(), this.getJmxPort(), "Port and JMX port must be different.");
        this.rack = Strings.isNullOrEmpty(rack) ? "NA" : rack.trim();
    }

    @Override
    public ByteString getContent() {
        final String message = this.toString();
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }

    public String toString() {
        return "JoinNode id=%s host=%s port=%s jmxPort=%s rack=%s".formatted(
                id, host, port, jmxPort, rack
        );
    }
}