package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.ReplicaRole;
import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.Table;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

@Slf4j
public class PartitionLeaderElector {
    public static void elect(Table table, Map<String, Object> storageNodes) {
        for (Partition partition : table.getPartitions().values()) {
            elect(table.getName(), partition, storageNodes);
        }
    }

    public static void oustAndReelect(StorageNode storageNode, Map<String, Object> tables, Map<String, Object> storageNodes) {
        storageNode.getReplicaAssignmentSet().forEach(replicaAssignment -> {
            Table table = (Table)tables.get(replicaAssignment.getTableName());
            table.getPartitions().values().forEach(partition -> {
                if (storageNode.getId().equals(partition.getLeader())) {
                    partition.setLeader(null);
                    partition.getInSyncReplicas().remove(storageNode.getId());
                    log.info("Storage node '{}' removed from leadership and ISR list of table '{}' partition '{}'.",
                            storageNode.getId(),
                            table.getName(),
                            partition.getId());
                    elect(table.getName(), partition, storageNodes);
                }
            });
        });
    }

    private static void elect(String tableName, Partition partition, Map<String, Object> storageNodes) {
        if (partition.getInSyncReplicas().isEmpty()) {
            log.warn("Table '{}' partition '{}' has no in-sync replicas. Marking it as offline.",
                    tableName,
                    partition.getId());
            partition.setLeader(null);
            return;
        }

        var priorityQueue = new PriorityQueue<StorageNode>(Comparator.comparingLong(node ->
                node.getReplicaAssignmentSet().stream().filter(assignment ->
                        assignment.getRole() == ReplicaRole.LEADER).count()));

        priorityQueue.addAll(storageNodes.values().stream().map(StorageNode.class::cast).filter(node ->
                partition.getInSyncReplicas().contains(node.getId())).toList());

        partition.setLeader(priorityQueue.peek().getId());
        StorageNode electedStorageNode = (StorageNode)storageNodes.get(priorityQueue.peek().getId());
        electedStorageNode.getReplicaAssignmentSet().stream().filter(assignment ->
                        assignment.getTableName().equals(tableName) &&
                        assignment.getPartitionIid() == partition.getId())
                .findFirst()
                .get()
                .setRole(ReplicaRole.LEADER);

        log.info("Storage node '{}' elected as leader for table '{}' partition '{}'.",
                partition.getLeader(),
                tableName,
                partition.getId());
    }
}
