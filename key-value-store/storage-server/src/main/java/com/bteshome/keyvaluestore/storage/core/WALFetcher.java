package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.requests.WALFetchRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchPayloadType;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
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
    State state;

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

        List<Replica> followedReplicas = MetadataCache.getInstance().getFollowedReplicas(state.getNodeId());

        for (Replica followedReplica : followedReplicas) {
            try {
                String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(followedReplica.getTable(), followedReplica.getPartition());

                if (leaderEndpoint == null) {
                    log.trace("No leader for table '{}' partition '{}'. Skipping fetch.", followedReplica.getTable(), followedReplica.getPartition());
                    continue;
                }

                PartitionState partitionState = state.getPartitionState(followedReplica.getTable(), followedReplica.getPartition());

                if (partitionState == null)
                    continue;

                LogPosition lastFetchOffset = partitionState.getOffsetState().getEndOffset();
                int maxNumRecords = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_MAX_NUM_RECORDS_KEY);

                WALFetchRequest request = new WALFetchRequest(
                        state.getNodeId(),
                        followedReplica.getTable(),
                        followedReplica.getPartition(),
                        lastFetchOffset,
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
                    // TODO - change to error
                    log.trace("Error fetching WAL for table '{}' partition '{}'. Response is null.", followedReplica.getTable(), followedReplica.getPartition());
                    continue;
                }

                if (response.getHttpStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR.value() || response.getHttpStatusCode() == HttpStatus.BAD_REQUEST.value()) {
                    // TODO - change to error
                    log.trace("Error fetching WAL for table '{}' partition '{}'. Http status: {}, error: {}.",
                            followedReplica.getTable(),
                            followedReplica.getPartition(),
                            response.getHttpStatusCode(),
                            response.getErrorMessage());
                }

                if (response.getHttpStatusCode() == HttpStatus.CONFLICT.value()) {
                    log.info("Received a truncate request from the new leader for table '{}' partition '{}'. Truncating to offset '{}'.",
                            followedReplica.getTable(),
                            followedReplica.getPartition(),
                            response.getTruncateToOffset());
                    partitionState.getWal().truncateToAfterExclusive(response.getTruncateToOffset());
                    partitionState.getOffsetState().setEndOffset(response.getTruncateToOffset());
                    continue;
                }

                if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                    if (response.getPayloadType().equals(WALFetchPayloadType.LOG)) {
                        partitionState.appendLogEntries(
                                response.getEntries(),
                                response.getCommitedOffset());

                        log.trace("Fetched WAL for table '{}' partition '{}' lastFetchedOffset '{}'. entries={}, commited offset={}.",
                                followedReplica.getTable(),
                                followedReplica.getPartition(),
                                lastFetchOffset,
                                response.getEntries(),
                                response.getCommitedOffset());
                    } else {
                        partitionState.applyDataSnapshot(response.getDataSnapshot());

                        log.trace("Fetched data snapshot for table '{}' partition '{}' lastFetchedOffset '{}', last snapshot committed offset={}.",
                                followedReplica.getTable(),
                                followedReplica.getPartition(),
                                lastFetchOffset,
                                response.getDataSnapshot().getLastCommittedOffset());
                    }
                }
            } catch (Exception e) {
                // TODO - change to error
                log.trace("Error fetching WAL for table '{}' partition '{}'.", followedReplica.getTable(), followedReplica.getPartition(), e);
            }
        }
    }
}
