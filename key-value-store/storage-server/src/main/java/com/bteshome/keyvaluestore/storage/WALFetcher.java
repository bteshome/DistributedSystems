package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.requests.WALFetchRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
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
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_INTERVAL_MS_KEY);
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
                String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(followedReplica.getTable(), followedReplica.getPartition());

                if (leaderEndpoint == null) {
                    log.trace("No leader for table '{}' partition '{}'. Skipping fetch.", followedReplica.getTable(), followedReplica.getPartition());
                    continue;
                }

                long lastFetchedOffset = store.getReplicaEndOffset(followedReplica.getTable(), followedReplica.getPartition(), store.getNodeId());
                int lastFetchedLeaderTerm = store.getLastFetchedLeaderTerm(followedReplica.getTable(), followedReplica.getPartition(), store.getNodeId());
                int maxNumRecords = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_MAX_NUM_RECORDS_KEY);

                WALFetchRequest request = new WALFetchRequest(
                        store.getNodeId(),
                        followedReplica.getTable(),
                        followedReplica.getPartition(),
                        lastFetchedOffset,
                        lastFetchedLeaderTerm,
                        maxNumRecords
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

                if (response == null) {
                    log.error("Error fetching WAL for table '{}' partition '{}'. Response is null.", followedReplica.getTable(), followedReplica.getPartition());
                    continue;
                }

                if (response.getHttpStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR.value() || response.getHttpStatusCode() == HttpStatus.BAD_REQUEST.value()) {
                    log.error("Error fetching WAL for table '{}' partition '{}'. Http status: {}, error: {}.",
                            followedReplica.getTable(),
                            followedReplica.getPartition(),
                            response.getHttpStatusCode(),
                            response.getErrorMessage());
                }

                if (response.getHttpStatusCode() != HttpStatus.OK.value()) {
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

                store.applyLogEntries(request.getTable(), request.getPartition(), response.getEntries());
            } catch (Exception e) {
                log.error("Error fetching WAL for table '{}' partition '{}'.", followedReplica.getTable(), followedReplica.getPartition(), e);
            }
        }
    }
}
