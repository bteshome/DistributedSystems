package com.bteshome.keyvaluestore.common.responses;

import com.bteshome.keyvaluestore.common.JavaSerDe;
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
public class GenericResponse implements Serializable, Message {
    private int httpStatusCode;
    private String message;
    @Serial
    private static final long serialVersionUID = 1L;

    public GenericResponse() {
        this.httpStatusCode = 200;
        this.message = "";
    }

    public GenericResponse(int httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
        this.message = "";
    }

    public GenericResponse(int httpStatusCode, String message) {
        this.httpStatusCode = httpStatusCode;
        this.message = message;
    }

    @Override
    public ByteString getContent() {
        final String message = httpStatusCode + " " + JavaSerDe.serialize(this);
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}