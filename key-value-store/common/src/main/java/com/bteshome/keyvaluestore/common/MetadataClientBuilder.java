package com.bteshome.keyvaluestore.common;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.util.TimeDuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

        // TODO - these timeout settings don't work. May be on Linux only.
        //properties.setTimeDuration("ratis.rpc.request.timeout", TimeDuration.valueOf(3, TimeUnit.SECONDS));
        //properties.setTimeDuration("ratis.rpc.client.call.timeout", TimeDuration.valueOf(3, TimeUnit.SECONDS));

        return RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setClientId(_clientId)
                .setClientRpc(new NettyFactory(new Parameters()).newRaftClientRpc(_clientId, properties))
                .build();
    }
}
