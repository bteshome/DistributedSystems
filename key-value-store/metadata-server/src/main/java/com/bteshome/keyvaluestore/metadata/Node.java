package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.metadata.common.Settings;
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
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class Node implements CommandLineRunner {
    private RaftServer server = null;

    @Autowired
    Settings settings;

    private RaftPeer buildPeer(Map<String, String> peerInfo) {
        String id = peerInfo.get("id");
        String host = peerInfo.get("host");
        String port = peerInfo.get("port");
        return RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(id))
                .setAddress(host + ":" + port)
                .build();
    }

    private void deleteDirectoryIfItExists() {
        Path directoryToDelete = Paths.get(settings.getStorageDir());

        try {
            Files.walkFileTree(directoryToDelete, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            System.err.println("Error deleting directory: " + e.getMessage());
        }
    }

    private void stopServer() {
        if (server == null) {
            return;
        }

        try {
            server.close();
        } catch (IOException e) {
            log.info("Error stopping server", e);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            deleteDirectoryIfItExists();

            RaftPeer node = buildPeer(settings.getNode());
            List<RaftPeer> peers = settings.getPeers().stream().map(this::buildPeer).toList();
            RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(settings.getGroupId()), peers);
            MyStateMachine stateMachine = new MyStateMachine();

            RaftProperties properties = new RaftProperties();
            NettyConfigKeys.Server.setHost(properties, settings.getNode().get("host"));
            NettyConfigKeys.Server.setPort(properties, Integer.parseInt(settings.getNode().get("port")));

            properties.set("ratis.server.replication.factor", "1");
            properties.set("raft.server.storage.dir", settings.getStorageDir());
            properties.set("raft.rpc.type", "NETTY");

            server = RaftServer.newBuilder()
                    .setProperties(properties)
                    .setServerId(node.getId())
                    .setGroup(group)
                    .setStateMachine(stateMachine)
                    .setOption(RaftStorage.StartupOption.FORMAT)
                    .build();

            System.out.println("\n\n");
            log.info("Server properties: {}", server.getProperties());
            System.out.println("\n\n");

            System.out.println("now starting...");

            server.start();

            Thread mainThread = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    log.info("System shutdown detected. Stopping server...");
                    stopServer();
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        log.info("Interrupted while waiting for main thread to complete", e);
                    }
                }
            });
        } catch (Exception e) {
            log.error("Unhandled error: ", e);
            stopServer();
        }
    }
}
