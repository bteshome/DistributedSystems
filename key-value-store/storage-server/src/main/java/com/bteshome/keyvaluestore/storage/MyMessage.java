package com.bteshome.keyvaluestore.storage;
import org.apache.ratis.netty.NettyUtils;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import org.apache.ratis.util.ProtoUtils;
import org.bouncycastle.util.encoders.UTF8;

import java.nio.charset.StandardCharsets;

public class MyMessage implements Message {
    @Override
    public ByteString getContent() {
        final String msg = "PUT foo=bar";
        byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
        return ProtoUtils.toByteString(bytes);
    }
}
