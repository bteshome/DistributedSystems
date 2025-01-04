package com.bteshome.keyvaluestore.adminclient.common;

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
public class Client {
    @Autowired
    private MetadataSettings metadataSettings;

    @Autowired
    private Settings settings;

    public RaftClient createRaftClient() {
        List<RaftPeer> peers = metadataSettings.getPeers().stream().map(this::buildPeer).toList();
        RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(metadataSettings.getGroupId()), peers);
        RaftProperties properties = new RaftProperties();
        ClientId clientId = ClientId.valueOf(settings.getClientId());

        return RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setClientId(clientId)
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
