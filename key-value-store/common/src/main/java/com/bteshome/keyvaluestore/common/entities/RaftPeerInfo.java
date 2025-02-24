package com.bteshome.keyvaluestore.common.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.ratis.proto.RaftProtos;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RaftPeerInfo implements Serializable {
    private String address;
    private String id;
    RaftProtos.RaftPeerRole role;
}
