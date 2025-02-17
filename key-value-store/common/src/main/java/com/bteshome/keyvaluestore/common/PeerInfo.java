package com.bteshome.keyvaluestore.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PeerInfo {
    private String id;
    private String host;
    private int port;

    @Override
    public String toString() {
        return String.format("id=%s,host=%s,port=%d", id, host, port);
    }
}
