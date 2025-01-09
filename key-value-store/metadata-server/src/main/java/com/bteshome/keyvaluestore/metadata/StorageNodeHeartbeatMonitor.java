package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.StorageNodeState;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class StorageNodeHeartbeatMonitor {
    private boolean checkStatus(StorageNode node, long expectIntervalMs) {
        if (node.getLastHeartbeatReceivedTime() < System.nanoTime() - 1000000L * expectIntervalMs) {
            if (node.getState() == StorageNodeState.ACTIVE) {
                // TODO - more needs to be done here, for e.g.:
                //  - electing a different leader if it is a leader for any partitions
                //  - removing it from the isr list if it's a replica for any paritions
                log.warn("Storage node '{}' has not sent a heartbeat in {} ms. Marking it as inactive.", node.getId(), expectIntervalMs);
                node.setState(StorageNodeState.INACTIVE);
                return true;
            }
        } else {
            if  (node.getState() == StorageNodeState.INACTIVE) {
                // TODO - more needs to be done here., for e.g.: ...
                log.info("Started receiving heartbeat from storage node '{}'. Marking it as active", node.getId());
                node.setState(StorageNodeState.ACTIVE);
                return true;
            }
        }

        return false;
    }

    public boolean checkStatus(Map<String, Object> storageNodes, long expectIntervalMs) {
        log.info("Storage node heartbeat monitor fired. Checking...");
        return storageNodes
                .values()
                .stream()
                .map(StorageNode.class::cast)
                .anyMatch(node -> checkStatus(node, expectIntervalMs));
    }
}
