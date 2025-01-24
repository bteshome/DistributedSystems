package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Stream;

@Slf4j
public class PartitionLeaderElector {
    public static void elect(Table table, Map<String, Object> storageNodes) {
        for (Partition partition : table.getPartitions().values()) {
            elect(partition, storageNodes);
        }
    }

    public static List<Partition> oustAndReelect(StorageNode storageNode, Map<String, Object> tables, Map<String, Object> storageNodes) {
        Stream<Partition> partitionsThatNeedReelection = storageNode.getReplicaAssignmentSet()
                .stream()
                .filter(ReplicaAssignment::isLeader)
                .flatMap(replicaAssignment -> {
                    replicaAssignment.setRole(ReplicaRole.FOLLOWER);
                    return ((Table)tables.get(replicaAssignment.getTableName()))
                            .getPartitions()
                            .values()
                            .stream()
                            .filter(partition -> storageNode.getId().equals(partition.getLeader()))
                            .peek(partition -> {
                                partition.setLeader(null);
                                partition.getInSyncReplicas().remove(storageNode.getId());
                                log.info("Storage node '{}' removed from leadership and ISR list of table '{}' partition '{}'.",
                                        storageNode.getId(),
                                        partition.getTableName(),
                                        partition.getId());
                            });
                });

        return partitionsThatNeedReelection
                .peek( partition  -> elect(partition, storageNodes))
                .toList();
    }

    private static void elect(Partition partition, Map<String, Object> storageNodes) {
        var priorityQueue = new PriorityQueue<StorageNode>(Comparator.comparingLong(node ->
                node.getReplicaAssignmentSet().stream().filter(assignment ->
                        assignment.getRole() == ReplicaRole.LEADER).count()));

        priorityQueue.addAll(storageNodes.values().stream().map(StorageNode.class::cast)
                        .filter(StorageNode::isActive)
                        .filter(node -> partition.getInSyncReplicas().contains(node.getId())).toList());

        if (priorityQueue.isEmpty()) {
            log.warn("Table '{}' partition '{}' has no active, in-sync replicas. Marking it as offline.",
                    partition.getTableName(),
                    partition.getId());
            partition.setLeader(null);
            return;
        }

        partition.setLeader(priorityQueue.peek().getId());
        partition.setLeaderTerm(partition.getLeaderTerm() + 1);
        StorageNode electedStorageNode = (StorageNode)storageNodes.get(priorityQueue.peek().getId());
        electedStorageNode.getReplicaAssignmentSet().stream()
                .filter(assignment ->
                        assignment.getTableName().equals(partition.getTableName()) &&
                        assignment.getPartitionIid() == partition.getId())
                .findFirst()
                .get()
                .setRole(ReplicaRole.LEADER);

        log.info("Storage node '{}' elected as leader for table '{}' partition '{}' in term '{}'.",
                partition.getLeader(),
                partition.getTableName(),
                partition.getId(),
                partition.getLeaderTerm());
    }
}
