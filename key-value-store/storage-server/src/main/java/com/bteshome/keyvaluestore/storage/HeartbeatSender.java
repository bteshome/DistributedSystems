package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.requests.StorageNodeHeartbeatRequest;
import com.bteshome.keyvaluestore.common.responses.StorageNodeHeartbeatResponse;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

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
    MetadataClientBuilder metadataClientBuilder;

    @Autowired
    MetadataRefresher metadataRefresher;

    @Autowired
    State state;

    @PreDestroy
    public void close() {
        if (executor != null) {
            executor.close();
        }
    }

    public void schedule() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.STORAGE_NODE_HEARTBEAT_SEND_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::sendHeartbeat, 0L, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled heartbeat sender. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling heartbeat sender.", e);
        }
    }

    private void sendHeartbeat() {
        try {
            StorageNodeHeartbeatRequest request = new StorageNodeHeartbeatRequest(
                    storageSettings.getNode().getId(),
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
            state.setLastHeartbeatSucceeded(true);
            if (response.isLaggingOnMetadata()) {
                log.warn("The node is lagging behind on metadata. Now issuing a fetch request.");
                metadataRefresher.fetch();
            }
        } catch (Exception e) {
            state.setLastHeartbeatSucceeded(false);
            log.error("Error sending heartbeat: ", e);
        }
    }
}