package com.bteshome.keyvaluestore.common;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.protocol.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class MetadataClientBuilder {
    @Autowired
    private MetadataClientSettings metadataClientSettings;

    public RaftClient createRaftClient() {
        return createClient(
                metadataClientSettings.getPeers(),
                metadataClientSettings.getGroupId(),
                metadataClientSettings.getClientId());
    }

    public static RaftClient createRaftClient(
            List<PeerInfo> peerInfoList,
            UUID groupId,
            UUID clientId) {
        return createClient(peerInfoList, groupId, clientId);
    }

    private static RaftClient createClient(
            List<PeerInfo> peerInfoList,
            UUID  groupId,
            UUID clientId) {
        List<RaftPeer> peers = peerInfoList.stream().map(peerInfo ->
                RaftPeer
                        .newBuilder()
                        .setId(RaftPeerId.valueOf(peerInfo.getId()))
                        .setAddress(peerInfo.getHost() + ":" + peerInfo.getPort())
                        .build())
                .toList();
        RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
        RaftProperties properties = new RaftProperties();
        ClientId _clientId = ClientId.valueOf(clientId);

        return RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setClientId(_clientId)
                .setClientRpc(new NettyFactory(new Parameters()).newRaftClientRpc(_clientId, properties))
                .build();
    }
}
