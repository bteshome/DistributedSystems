package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.common.MetadataClientBuilder;
import com.bteshome.keyvaluestore.common.MetadataRefresher;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.requests.StorageNodeJoinRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class Node implements CommandLineRunner {
    @Autowired
    MetadataClientBuilder metadataClientBuilder;

    @Autowired
    StorageSettings storageSettings;

    @Autowired
    HeartbeatSender heartbeatSender;

    @Autowired
    MetadataRefresher metadataRefresher;

    @Autowired
    ReplicaMonitor replicaMonitor;

    @Autowired
    WALFetcher walFetcher;

    private final Map<String, Map<Integer, Map<String, String>>> state = new HashMap<>();

    @Override
    public void run(String... args) throws IOException {
        try (RaftClient client = this.metadataClientBuilder.createRaftClient()) {
            StorageNodeJoinRequest request = new StorageNodeJoinRequest(
                    storageSettings.getNode().getId(),
                    storageSettings.getNode().getHost(),
                    storageSettings.getNode().getPort(),
                    storageSettings.getNode().getJmxPort(),
                    storageSettings.getNode().getRack(),
                    storageSettings.getNode().getStorageDir());
            log.info("Trying to join cluster '{}' with node id: '{}'", client.getGroupId().getUuid(), request.getId());
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                    log.info(response.getMessage());
                    metadataRefresher.fetch();
                    heartbeatSender.schedule();
                    metadataRefresher.schedule();
                    //replicaMonitor.schedule();
                    walFetcher.schedule();
                } else {
                    log.error(response.getMessage());
                }
            } else {
                log.error("Error joining cluster: ", reply.getException());
            }
        }
    }
}
