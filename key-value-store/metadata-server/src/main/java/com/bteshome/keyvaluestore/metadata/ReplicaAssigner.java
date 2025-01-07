package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.ReplicaAssignment;
import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.Table;
import org.springframework.util.comparator.Comparators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class ReplicaAssigner {
    public void assign(Table table, List<StorageNode> availableStorageNodes) {
        var priorityQueue = new PriorityQueue<StorageNode>(Comparator.comparingInt(node -> node.getReplicaAssignmentSet().size()));

        priorityQueue.addAll(availableStorageNodes);

        for (Partition partition : table.getPartitionList()) {
            List<StorageNode> alreadyAssigned = new ArrayList<>();
            for (int replicaOrder = 1; replicaOrder <= table.getReplicationFactor(); replicaOrder++) {
                var storageNode = priorityQueue.poll();
                partition.getReplicas().add(storageNode.getId());
                partition.getInSyncReplicas().add(storageNode.getId());
                storageNode.getReplicaAssignmentSet().add(new ReplicaAssignment(table.getName(), partition.getId()));
                alreadyAssigned.add(storageNode);
            }
            priorityQueue.addAll(alreadyAssigned);
        }
    }
}
