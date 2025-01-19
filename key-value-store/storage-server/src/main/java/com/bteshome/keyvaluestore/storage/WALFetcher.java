package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.requests.WALAcknowledgeRequest;
import com.bteshome.keyvaluestore.storage.requests.WALFetchRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class WALFetcher {
    private ScheduledExecutorService executor = null;

    @Autowired
    State store;

    @PreDestroy
    public void close() {
        if (executor != null) {
            executor.close();
        }
    }

    public void schedule() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.WAL_FETCH_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::fetch, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled replica WAL fetcher. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replica WAL fetcher.", e);
        }
    }

    private void fetch() {
        log.trace("WAL fetcher triggered. Fetching ...");

        List<Replica> followedReplicas = MetadataCache.getInstance().getFollowedReplicas(store.getNodeId());

        for (Replica followedReplica : followedReplicas) {
            try {
                long lastFetchedOffset = store.getEndOffset(followedReplica.getTable(), followedReplica.getPartition(), store.getNodeId());
                String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(followedReplica.getTable(), followedReplica.getPartition());

                if (leaderEndpoint == null) {
                    log.trace("No leader for table '{}' partition '{}'. Skipping fetch.", followedReplica.getTable(), followedReplica.getPartition());
                    continue;
                }

                WALFetchRequest request = new WALFetchRequest(
                        store.getNodeId(),
                        followedReplica.getTable(),
                        followedReplica.getPartition(),
                        lastFetchedOffset
                );

                WALFetchResponse response = RestClient.builder()
                        .build()
                        .post()
                        .uri("http://%s/api/wal/fetch/".formatted(leaderEndpoint))
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(request)
                        .retrieve()
                        .toEntity(WALFetchResponse.class)
                        .getBody();

                if (response.getHttpStatus() == HttpStatus.INTERNAL_SERVER_ERROR) {
                    log.error("Error fetching WAL for table '{}' partition '{}'. Http status: {}, error: {}.",
                            followedReplica.getTable(),
                            followedReplica.getPartition(),
                            response.getHttpStatus(),
                            response.getErrorMessage());
                    continue;
                }

                if (response.getHttpStatus() != HttpStatus.OK) {
                    continue;
                }

                log.debug("Fetched WAL for table '{}' partition '{}' lastFetchedOffset '{}'. entries={}, endOffsets={}, commited offset={}.",
                        followedReplica.getTable(),
                        followedReplica.getPartition(),
                        lastFetchedOffset,
                        response.getEntries(),
                        response.getReplicaEndOffsets(),
                        response.getCommitedOffset());

                store.appendLogEntries(
                        followedReplica.getTable(),
                        followedReplica.getPartition(),
                        response.getEntries(),
                        response.getReplicaEndOffsets(),
                        response.getCommitedOffset());

                if (response.getEntries().isEmpty()) {
                    continue;
                }

                long endOffset = store.getEndOffset(request.getTable(), request.getPartition(), store.getNodeId());
                acknowledgeFetch(request.getTable(), request.getPartition(), endOffset, leaderEndpoint);
                store.applyLogEntries(request.getTable(), request.getPartition(), response.getEntries());
            } catch (Exception e) {
                log.error("Error fetching WAL for table '{}' partition '{}'.", followedReplica.getTable(), followedReplica.getPartition(), e);
            }
        }
    }

    private void acknowledgeFetch(String table, int partition, long endOffset, String leaderEndpoint) {
        WALAcknowledgeRequest request = new WALAcknowledgeRequest(
                store.getNodeId(),
                table,
                partition,
                endOffset);

        ResponseEntity<String> response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/wal/acknowledge/".formatted(leaderEndpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(String.class);

        if (response.getStatusCode().value() != HttpStatus.OK.value()) {
            log.error("Error acknowledging WAL fetch for table '{}' partition '{}'. Http status: {}, error: {}.",
                    table,
                    partition,
                    response.getStatusCode().value(),
                    response.getBody());
        } else {
            log.debug("Acknowledged WAL fetch for table '{}' partition '{}'.", table, partition);
        }
    }
}
