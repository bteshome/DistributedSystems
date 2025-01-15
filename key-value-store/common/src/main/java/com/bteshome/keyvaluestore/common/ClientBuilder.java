package com.bteshome.keyvaluestore.common;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.protocol.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class ClientBuilder {
    @Autowired
    private ClientSettings clientSettings;

    public RaftClient createRaftClient() {
        List<RaftPeer> peers = clientSettings.getPeers().stream().map(peerInfo ->
                RaftPeer
                        .newBuilder()
                        .setId(RaftPeerId.valueOf(peerInfo.getId()))
                        .setAddress(peerInfo.getHost() + ":" + peerInfo.getPort())
                        .build())
                .toList();
        RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(clientSettings.getGroupId()), peers);
        RaftProperties properties = new RaftProperties();
        ClientId clientId = ClientId.valueOf(clientSettings.getClientId());

        return RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setClientId(clientId)
                .setClientRpc(new NettyFactory(new Parameters()).newRaftClientRpc(clientId, properties))
                .build();
    }
}
