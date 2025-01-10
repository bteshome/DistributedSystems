package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.Table;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class PartitionLeaderElector {
    public static void elect(Table table) {
        for (Partition partition : table.getPartitions().values()) {
            elect(table.getName(), partition);
        }
    }

    public static void oust(StorageNode storageNode, Map<String, Object> tables) {
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
                    elect(table.getName(), partition);
                }
            });
        });
    }

    private static void elect(String tableName, Partition partition) {
        if (partition.getInSyncReplicas().isEmpty()) {
            log.warn("Table '{}' partition '{}' has no in-sync replicas. Marking it as offline.",
                    tableName,
                    partition.getId());
            partition.setLeader(null);
            return;
        }

        partition.setLeader(partition.getInSyncReplicas().getFirst());
        log.info("Storage node '{}' elected as leader for table '{}' partition '{}'.",
                partition.getLeader(),
                tableName,
                partition.getId());
    }
}
