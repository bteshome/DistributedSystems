package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.requests.StorageNodeHeartbeatRequest;
import com.bteshome.keyvaluestore.common.responses.StorageNodeHeartbeatResponse;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

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
    StorageNodeMetadataRefresher metadataRefresher;
    @Autowired
    State state;
    @Autowired
    WebClient webClient;

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

            Mono<ResponseEntity<StorageNodeHeartbeatResponse>> mono = webClient
                    .post()
                    .uri("http://%s".formatted(MetadataCache.getInstance().getHeartbeatEndpoint()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .bodyValue(request)
                    .retrieve()
                    .toEntity(StorageNodeHeartbeatResponse.class);

            mono.subscribe(responseEntity -> {
                StorageNodeHeartbeatResponse response = responseEntity.getBody();

                if (response == null) {
                    log.error("Heartbeat request failed. The response is null.");
                    return;
                }

                switch (HttpStatus.valueOf(response.getHttpStatusCode())) {
                    case OK -> {
                        log.trace("Sent heartbeat successfully");
                        if (response.isLaggingOnMetadata()) {
                            log.debug("The node is lagging on metadata. Now issuing a fetch request.");
                            metadataRefresher.fetch();
                        }
                    }
                    case MOVED_PERMANENTLY -> metadataRefresher.fetch();
                    default -> log.error("Heartbeat request failed. The response status is '{}', error message is '{}'.",
                            response.getHttpStatusCode(),
                            response.getErrorMessage());
                }
            });
        } catch (Exception e) {
            log.error("Error sending heartbeat: ", e);
        }
    }
}