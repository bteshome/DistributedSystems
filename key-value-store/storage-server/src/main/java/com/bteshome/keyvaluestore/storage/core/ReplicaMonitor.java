package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
        log.debug("Replica monitor about to check if any replicas are lagging on fetch.");

        try {
            String leaderNodeId = state.getNodeId();
            List<Tuple<String, Integer>> ownedPartitions = MetadataCache.getInstance().getOwnedPartitions(leaderNodeId);
            Set<Replica> laggingReplicas = new HashSet<>();
            Set<Replica> caughtUpReplicas = new HashSet<>();

            long recordThreshold = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_RECORDS_KEY);

            for (Tuple<String, Integer> ownedPartition : ownedPartitions) {
                String table = ownedPartition.first();
                int partition = ownedPartition.second();
                Set<String> allReplicaNodeIds = MetadataCache.getInstance().getReplicaNodeIds(table, partition);
                Set<String> inSyncReplicaNodeIds = MetadataCache.getInstance().getInSyncReplicas(table, partition);
                PartitionState partitionState = state.getPartitionState(table, partition, true);
                LogPosition committedOffset = partitionState.getOffsetState().getCommittedOffset();

                for (String replicaId : allReplicaNodeIds) {
                    if (replicaId.equals(leaderNodeId))
                        continue;

                    LogPosition replicaOffset = partitionState.getOffsetState().getReplicaEndOffset(replicaId);
                    long lag = partitionState.getWal().getLag(replicaOffset, committedOffset);
                    boolean isLaggingOnFetch = lag > recordThreshold;

                    if (inSyncReplicaNodeIds.contains(replicaId)) {
                        if (isLaggingOnFetch)
                            laggingReplicas.add(new Replica(replicaId, table, partition));
                    } else {
                        if (!isLaggingOnFetch)
                            caughtUpReplicas.add(new Replica(replicaId, table, partition));
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
