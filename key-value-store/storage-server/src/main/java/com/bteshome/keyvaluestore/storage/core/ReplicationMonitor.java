package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.LogPosition;
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

        Map<String, LogPosition> replicaEndOffsets = leaderPartitionState.getOffsetState().getReplicaEndOffsets();
        replicaEndOffsets.put(nodeId, endOffset);

        if (committedOffset.isLessThan(endOffset)) {
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

            committedOffset = upToDateReplicas.poll().getValue();
        }

        for (String replicaId : allReplicaIds) {
            if (replicaId.equals(nodeId))
                continue;

            if (!replicaEndOffsets.containsKey(replicaId))
                continue;

            if (replicaEndOffsets.get(replicaId).isGreaterThanOrEquals(committedOffset)) {
                if (!inSyncReplicaIds.contains(replicaId))
                    caughtUpReplicas.add(new Replica(replicaId, table, partition));
            } else {
                if (inSyncReplicaIds.contains(replicaId)) {
                    // TODO - this needs work. not all offsets are being added to the hashmap
                    //long timeLag = System.currentTimeMillis() - leaderPartitionState.getOffsetState().getLogTimestamp(committedOffset);
                    //if (timeLag > timeLagThresholdMs) {
                        //laggingReplicas.add(new Replica(replicaId, table, partition));
                    //} else {
                        // TODO - how do we determine the right record lag threshold?
                        long recordLag = leaderPartitionState.getWal().getLag(replicaEndOffsets.get(replicaId), committedOffset);
                        if (recordLag > recordLagThreshold)
                            laggingReplicas.add(new Replica(replicaId, table, partition));
                    //}
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
