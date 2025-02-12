package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.Tuple;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class ReplicationMonitor {
    public static LogPosition check(PartitionState leaderPartitionState,
                                    String nodeId,
                                    String table,
                                    int partition,
                                    int minISRCount,
                                    Set<String> allReplicaIds,
                                    Set<String> inSyncReplicaIds,
                                    long timeLagThresholdMs,
                                    long recordLagThreshold,
                                    ISRSynchronizer isrSynchronizer) {
        log.trace("Checking replication status for table {} partition {}.", table, partition);

        Set<Replica> caughtUpReplicas = new HashSet<>();
        Set<Replica> laggingReplicas = new HashSet<>();
        LogPosition committedOffset = leaderPartitionState.getOffsetState().getCommittedOffset();
        LogPosition endOffset = leaderPartitionState.getOffsetState().getEndOffset();
        Map<String, Tuple<LogPosition, Long>> replicaEndOffsets = leaderPartitionState.getOffsetState().getReplicaEndOffsets();

        if (committedOffset.isLessThan(endOffset)) {
            PriorityQueue<LogPosition> upToDateReplicaOffsets = new PriorityQueue<>();
            upToDateReplicaOffsets.offer(endOffset);

            for (Tuple<LogPosition, Long> replicaEndOffset : replicaEndOffsets.values()) {
                if (upToDateReplicaOffsets.size() < minISRCount) {
                    upToDateReplicaOffsets.offer(replicaEndOffset.first());
                } else {
                    if (replicaEndOffset.first().isGreaterThan(upToDateReplicaOffsets.peek())) {
                        upToDateReplicaOffsets.poll();
                        upToDateReplicaOffsets.offer(replicaEndOffset.first());
                    }
                }
            }

            if (upToDateReplicaOffsets.size() == minISRCount)
                committedOffset = upToDateReplicaOffsets.peek();
        }

        for (String replicaId : allReplicaIds) {
            if (replicaId.equals(nodeId))
                continue;

            if (!replicaEndOffsets.containsKey(replicaId))
                continue;

            Tuple<LogPosition, Long> replicaEndOffset = replicaEndOffsets.get(replicaId);
            long timeLag = System.currentTimeMillis() - replicaEndOffset.second();
            boolean isLagging = timeLag > timeLagThresholdMs;

            if (!isLagging) {
                long recordLag = leaderPartitionState.getWal().getLag(replicaEndOffset.first(), committedOffset);
                isLagging = recordLag > recordLagThreshold;
            }

            if (!isLagging) {
                if (!inSyncReplicaIds.contains(replicaId))
                    caughtUpReplicas.add(new Replica(replicaId, table, partition));
            } else {
                if (inSyncReplicaIds.contains(replicaId)) {
                    laggingReplicas.add(new Replica(replicaId, table, partition));
                }
            }
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

        return committedOffset;
    }
}
