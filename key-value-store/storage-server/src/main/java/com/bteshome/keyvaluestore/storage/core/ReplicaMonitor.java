package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.requests.WALGetReplicaEndOffsetRequest;
import com.bteshome.keyvaluestore.storage.responses.WALGetReplicaEndOffsetResponse;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.config.RequestConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ReplicaMonitor {
    private ScheduledExecutorService executor = null;
    @Autowired
    State state;
    @Autowired
    ISRSynchronizer isrSynchronizer;

    @PreDestroy
    public void close() {
        if (executor != null)
            executor.close();
    }

    public void schedule() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_MONITOR_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::checkStatus, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled replica monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replica monitor.", e);
        }
    }

    private void checkStatus() {
        log.trace("Replica monitor about to check if any replicas are lagging on fetch.");

        try {
            String leaderNodeId = state.getNodeId();
            List<Tuple<String, Integer>> ownedPartitions = MetadataCache.getInstance().getOwnedPartitions(leaderNodeId);
            Set<Replica> laggingReplicas = new HashSet<>();
            Set<Replica> caughtUpReplicas = new HashSet<>();

            long recordThreshold = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_RECORDS_KEY);

            for (Tuple<String, Integer> ownedPartition : ownedPartitions) {
                String table = ownedPartition.first();
                int partition = ownedPartition.second();
                PartitionState partitionState = state.getPartitionState(table, partition);

                if (partitionState == null)
                    continue;

                LogPosition committedOffset = partitionState.getOffsetState().getCommittedOffset();
                Set<String> allReplicaNodeIds = MetadataCache.getInstance().getReplicaNodeIds(
                        table,
                        partition);
                Set<String> inSyncReplicaNodeIds = MetadataCache.getInstance().getInSyncReplicas(table, partition);
                WALGetReplicaEndOffsetRequest walGetReplicaEndOffsetRequest = new WALGetReplicaEndOffsetRequest(
                        table,
                        partition);

                for (String nodeId : allReplicaNodeIds) {
                    if (nodeId.equals(this.state.getNodeId()))
                        continue;

                    String endpoint = MetadataCache.getInstance().getEndpoint(nodeId);

                    try {
                        // TODO - 1. what should these numbers be? 2. should they be configurable?
                        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
                        factory.setConnectTimeout(1000);
                        factory.setConnectionRequestTimeout(1000);
                        factory.setReadTimeout(1000);
                        WALGetReplicaEndOffsetResponse response = RestClient.builder()
                                .requestFactory(factory)
                                .build()
                                .post()
                                .uri("http://%s/api/wal/get-end-offset/".formatted(endpoint))
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(walGetReplicaEndOffsetRequest)
                                .retrieve()
                                .toEntity(WALGetReplicaEndOffsetResponse.class)
                                .getBody();

                        long lag = partitionState.getWal().getLag(response.getEndOffset(), committedOffset);
                        boolean isLaggingOnFetch = lag > recordThreshold;

                        if (inSyncReplicaNodeIds.contains(nodeId)) {
                            if (isLaggingOnFetch)
                                laggingReplicas.add(new Replica(nodeId, table, partition));
                        } else {
                            if (!isLaggingOnFetch)
                                caughtUpReplicas.add(new Replica(nodeId, table, partition));
                        }
                    } catch (Exception e) {
                        log.warn("Error getting replica end offset from endpoint '{}' for table '{}' partition '{}'.",
                                endpoint,
                                table,
                                partition,
                                e);
                    }
                }
            }

            if (!laggingReplicas.isEmpty()) {
                log.debug("These replicas are lagging on fetch beyond the threshold '{}'. Preparing to remove them from ISR lists: '{}'",
                        recordThreshold,
                        laggingReplicas);
                isrSynchronizer.removeFromInSyncReplicaLists(laggingReplicas);
            }

            if (!caughtUpReplicas.isEmpty()) {
                log.debug("These replicas have caught up on fetch. Preparing to add them to ISR lists: '{}'", caughtUpReplicas);
                isrSynchronizer.addToInSyncReplicaLists(caughtUpReplicas);
            }
        } catch (Exception e) {
            log.error("Error checking replica status", e);
        }
    }
}
