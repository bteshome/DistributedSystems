package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import com.bteshome.keyvaluestore.common.requests.StorageNodeHeartbeatRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.common.responses.StorageNodeHeartbeatResponse;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class HeartbeatSender {
    private ScheduledExecutorService executor = null;

    @Autowired
    StorageSettings storageSettings;

    @Autowired
    ClientBuilder clientBuilder;

    @Autowired
    MetadataRefresher metadataRefresher;

    @PreDestroy
    public void close() {
        if (executor != null) {
            executor.close();
        }
    }

    public void schedule() {
        try {
            long interval = (Long) MetadataCache.getInstance()
                    .getState()
                    .get(EntityType.CONFIGURATION)
                    .get(ConfigKeys.STORAGE_NODE_HEARTBEAT_SEND_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::sendHeartbeat, 0L, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled heartbeat sender. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling heartbeat sender: ", e);
        }
    }

    private void sendHeartbeat() {
        try {
            log.debug("Trying to sending heartbeat");
            StorageNodeHeartbeatRequest request = new StorageNodeHeartbeatRequest(
                    storageSettings.getNode(),
                    MetadataCache.getInstance().getLastFetchedVersion());
            StorageNodeHeartbeatResponse response = RestClient.builder()
                    .build()
                    .post()
                    .uri("http://%s".formatted(MetadataCache.getInstance().getHeartbeatEndpoint()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(StorageNodeHeartbeatResponse.class);
            log.debug("Sent heartbeat successfully");
            if (response.isLaggingOnMetadata()) {
                log.warn("The node is lagging behind on metadata. Now issuing a fetch request.");
                metadataRefresher.fetch();
            }
        } catch (Exception e) {
            log.error("Error sending heartbeat: ", e);
        }

        /*try (RaftClient client = this.clientBuilder.createRaftClient()) {
            StorageNodeHeartbeatRequest request = new StorageNodeHeartbeatRequest(
                    storageSettings.getNode(),
                    MetadataCache.getInstance().getLastFetchedVersion());
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    StorageNodeHeartbeatResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    log.debug("Sent heartbeat successfully");
                    if (response.isLaggingOnMetadata()) {
                        log.warn("The node is lagging behind on metadata. Now issuing a fetch request.");
                        metadataRefresher.fetch();
                    }
                } else {
                    GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                    log.error("Error sending heartbeat: {}", response.getMessage());
                }
            } else {
                log.error("Error sending heartbeat: ", reply.getException());
            }
        } catch (Exception e) {
            log.error("Error sending heartbeat: ", e);
        }*/
    }
}