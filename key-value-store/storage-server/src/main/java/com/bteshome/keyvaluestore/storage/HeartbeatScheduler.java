package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.common.Client;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.requests.StorageNodeHeartbeatRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class HeartbeatScheduler implements CommandLineRunner {
    private ScheduledExecutorService executor = null;

    @Autowired
    StorageSettings storageSettings;

    @Autowired
    Client client;

    @Override
    public void run(String... args) throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                executor.close();
            }
        });
        executor.scheduleAtFixedRate(this::sendHeartbeat,
                storageSettings.getHeartbeatSendIntervalSeconds(),
                storageSettings.getHeartbeatSendIntervalSeconds(),
                TimeUnit.SECONDS);

        log.info("Scheduled heartbeat thread. Frequency is {} seconds.", storageSettings.getHeartbeatSendIntervalSeconds());
    }

    private void sendHeartbeat() {
        // metadata version needs work
        try (RaftClient client = this.client.createRaftClient()) {
            StorageNodeHeartbeatRequest request = new StorageNodeHeartbeatRequest(storageSettings.getNode(), "TODO");
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == ResponseStatus.OK) {
                    log.debug("Sent heartbeat successfully");
                } else {
                    GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                    log.error("Error sending heartbeat: {}", response.getMessage());
                }
            } else {
                log.error("Error sending heartbeat: ", reply.getException());
            }
        } catch (Exception e) {
            log.error("Error sending heartbeat: ", e);
        }
    }
}