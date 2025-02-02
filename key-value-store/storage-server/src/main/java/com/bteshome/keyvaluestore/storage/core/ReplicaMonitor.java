package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Component
@Slf4j
public class ReplicaMonitor implements ApplicationListener<ContextClosedEvent> {
    private ScheduledExecutorService scheduler = null;
    private ExecutorService worker = null;
    @Autowired
    State state;
    @Autowired
    ISRSynchronizer isrSynchronizer;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        if (scheduler != null)
            scheduler.close();
        if (worker != null)
            worker.close();
    }

    public void schedule() {
        try {
            // TODO - increase # of threads for the worker
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_MONITOR_INTERVAL_MS_KEY);
            scheduler = Executors.newSingleThreadScheduledExecutor();
            worker = Executors.newFixedThreadPool(4);
            scheduler.scheduleAtFixedRate(this::checkStatus, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled replica monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replica monitor.", e);
        }
    }

    private void checkStatus() {
        log.trace("Replica monitor about to check if any replicas are lagging on fetch.");

        try {
            final String leaderNodeId = state.getNodeId();
            final List<Tuple<String, Integer>> ownedPartitions = MetadataCache.getInstance().getOwnedPartitions(leaderNodeId);
            final Set<Replica> laggingReplicas = new HashSet<>();
            final Set<Replica> caughtUpReplicas = new HashSet<>();
            long recordLagThreshold = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_RECORDS_KEY);
            long timeLagThresholdMs = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_TIME_MS_KEY);

            for (Tuple<String, Integer> ownedPartition : ownedPartitions) {
                CompletableFuture.runAsync(() ->
                    checkStatusForAPartition(ownedPartition,
                                             timeLagThresholdMs,
                                             caughtUpReplicas,
                                             laggingReplicas),
                    worker);
            }

            if (!laggingReplicas.isEmpty()) {
                log.debug("These replicas are lagging on fetch '{}'. Preparing to remove them from ISR lists: '{}'",
                        recordLagThreshold,
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

    private void checkStatusForAPartition(Tuple<String, Integer> ownedPartition,
                                          long timeLagThresholdMs,
                                          Set<Replica> caughtUpReplicas,
                                          Set<Replica> laggingReplicas) {
        final String table = ownedPartition.first();
        final int partition = ownedPartition.second();
        final PartitionState partitionState = state.getPartitionState(table, partition);

        if (partitionState == null)
            return;

        LogPosition committedOffset = partitionState.getOffsetState().getCommittedOffset();
        LogPosition endOffset = partitionState.getOffsetState().getEndOffset();

        if (committedOffset.equals(endOffset))
            return;

        int minISRCount = MetadataCache.getInstance().getMinInSyncReplicas(table);
        Map<String, LogPosition> replicaEndOffsets = partitionState.getOffsetState().getReplicaEndOffsets();
        Set<String> allReplicaIds = MetadataCache.getInstance().getReplicaNodeIds(
                table,
                partition);
        Set<String> inSyncReplicaIds = MetadataCache.getInstance().getInSyncReplicas(table, partition);
        PriorityQueue<Map.Entry<String, LogPosition>> upToDateReplicas = new PriorityQueue<>(Map.Entry.comparingByValue());

        for (Map.Entry<String, LogPosition> replicaEndOffset : replicaEndOffsets.entrySet()) {
            if (upToDateReplicas.isEmpty() || upToDateReplicas.size() < minISRCount)
                upToDateReplicas.offer(replicaEndOffset);
            else {
                if (replicaEndOffset.getValue().isGreaterThan(upToDateReplicas.peek().getValue())) {
                    upToDateReplicas.poll();
                    upToDateReplicas.offer(replicaEndOffset);
                }
            }
        }

        while (upToDateReplicas.size() > 1)
            upToDateReplicas.poll();

        LogPosition newCommittedOffset = upToDateReplicas.poll().getValue();

        if (newCommittedOffset.isGreaterThan(committedOffset)) {
            partitionState.getOffsetState().setCommittedOffset(newCommittedOffset);

            for (String replicaId : allReplicaIds) {
                if (replicaId.equals(this.state.getNodeId()))
                    continue;

                if (replicaEndOffsets.get(replicaId).isGreaterThanOrEquals(newCommittedOffset)) {
                    if (!inSyncReplicaIds.contains(replicaId))
                        caughtUpReplicas.add(new Replica(replicaId, table, partition));
                } else {
                    if (inSyncReplicaIds.contains(replicaId)) {
                        long timeLag = System.currentTimeMillis() - partitionState.getOffsetState().getLogTimestamp(newCommittedOffset);
                        if (timeLag > timeLagThresholdMs)
                            laggingReplicas.add(new Replica(replicaId, table, partition));
                    }
                }
            }

            return;
        }

        for (String replicaId : allReplicaIds) {
            if (replicaId.equals(this.state.getNodeId()))
                continue;

            if (replicaEndOffsets.get(replicaId).isGreaterThanOrEquals(committedOffset)) {
                if (!inSyncReplicaIds.contains(replicaId))
                    caughtUpReplicas.add(new Replica(replicaId, table, partition));
            } else {
                if (inSyncReplicaIds.contains(replicaId)) {
                    long timeLag = System.currentTimeMillis() - partitionState.getOffsetState().getLogTimestamp(committedOffset);
                    if (timeLag > timeLagThresholdMs)
                        laggingReplicas.add(new Replica(replicaId, table, partition));
                }
            }
        }
    }
}
