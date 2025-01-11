package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.Utils;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class Node implements CommandLineRunner {
    private RaftServer server = null;

    @Autowired
    MetadataSettings metadataSettings;

    private RaftPeer buildPeer(Map<String, String> peerInfo) {
        String id = peerInfo.get("id");
        String host = peerInfo.get("host");
        String port = peerInfo.get("port");
        return RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(id))
                .setAddress(host + ":" + port)
                .build();
    }

    @PreDestroy
    private void stopServer() {
        try {
            if (server != null) {
                server.close();
            }
        } catch (IOException e) {
            log.info("Error stopping server", e);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            // TODO - don't delete this everytime
            Utils.deleteDirectoryIfItExists(metadataSettings.getStorageDir());

            RaftPeer node = buildPeer(metadataSettings.getNode());
            List<RaftPeer> peers = metadataSettings.getPeers().stream().map(this::buildPeer).toList();
            RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(metadataSettings.getGroupId()), peers);
            MetadataStateMachine stateMachine = new MetadataStateMachine(metadataSettings);

            RaftProperties properties = new RaftProperties();
            NettyConfigKeys.Server.setHost(properties, metadataSettings.getNode().get("host"));
            NettyConfigKeys.Server.setPort(properties, Integer.parseInt(metadataSettings.getNode().get("port")));

            // TODO - make these configurable
            properties.set("ratis.server.replication.factor", "1");
            properties.set("raft.server.storage.dir", metadataSettings.getStorageDir());
            properties.set("raft.rpc.type", "NETTY");
            properties.set("ratis.snapshot.auto.enable", "true");
            //properties.set("raft.server.snapshot.trigger-when-stop.enabled", "true");

            properties.set("raft.server.snapshot.auto.trigger.enabled ", "true");
            properties.set("raft.server.snapshot.auto.trigger.interval", "600000");
            properties.set("raft.server.snapshot.auto.trigger.threshold", "5");

            server = RaftServer.newBuilder()
                    .setProperties(properties)
                    .setServerId(node.getId())
                    .setGroup(group)
                    .setStateMachine(stateMachine)
                    .setOption(RaftStorage.StartupOption.FORMAT)
                    .build();

            server.start();
        } catch (Exception e) {
            log.error("Error starting server: ", e);
            stopServer();
        }
    }
}
