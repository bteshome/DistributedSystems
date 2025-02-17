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

@Getter
@Setter
public class StorageNodeJoinRequest implements Serializable, Message {
    private String id;
    private String host;
    private int port;
    private int managementPort;
    private String rack;
    private String storageDir;

    public StorageNodeJoinRequest(
            String id,
            String host,
            int port,
            int managementPort,
            String rack,
            String storageDir
    ) {
        this.id = Validator.notEmpty(id);
        this.host = Validator.notEmpty(host);
        this.port = Validator.inRange(port, 0, 65535);
        this.managementPort = Validator.inRange(managementPort, 0, 65535);
        this.rack = Validator.setDefault(rack, "NA");
        Validator.notEqual(this.getPort(), this.getManagementPort(), "Port", "Management port");
        this.storageDir = Validator.notEmpty(storageDir);
    }

    @Override
    public ByteString getContent() {
        final String message = MetadataRequestType.STORAGE_NODE_JOIN + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}