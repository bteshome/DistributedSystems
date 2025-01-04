package com.bteshome.keyvaluestore.storage;

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
public class Utils {
    @Autowired
    private AppSettings appSettings;

    public RaftClient createRaftClient() {
        List<RaftPeer> peers = appSettings.getPeers().stream().map(this::buildPeer).toList();
        RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(appSettings.getGroupId()), peers);
        RaftProperties properties = new RaftProperties();
        ClientId clientId = ClientId.valueOf(appSettings.getClientId());

        return RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setClientRpc(new NettyFactory(new Parameters()).newRaftClientRpc(clientId, properties))
                .build();
    }

    private RaftPeer buildPeer(Map<String, String> peerInfo) {
        String id = peerInfo.get("id");
        String host = peerInfo.get("host");
        String port = peerInfo.get("port");
        return RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(id))
                .setAddress(host + ":" + port)
                .build();
    }
}
