package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.StorageNodeStatus;
import com.bteshome.keyvaluestore.common.requests.StorageNodeActivateRequest;
import com.bteshome.keyvaluestore.common.requests.StorageNodeDeactivateRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.http.HttpStatus;

import java.nio.charset.StandardCharsets;

@Slf4j
public class StorageNodeHeartbeatMonitor {
    private MetadataSettings metadataSettings;

    private void checkStatus(String nodeId) {
        if (!UnmanagedState.heartbeats.containsKey(nodeId)) { return; }

        StorageNode node = (StorageNode)UnmanagedState.state.get(EntityType.STORAGE_NODE).get(nodeId);
        long expectIntervalMs = (Long)UnmanagedState.state.get(EntityType.CONFIGURATION).get(ConfigKeys.STORAGE_NODE_HEARTBEAT_EXPECT_INTERVAL_MS_KEY);
        boolean isLaggingOnHeartbeats = UnmanagedState.heartbeats.get(nodeId) < System.nanoTime() - 1000000L * expectIntervalMs;

        if (isLaggingOnHeartbeats) {
            if (!node.isActive()) { return; }

            // TODO - more needs to be done here, for e.g.:
            //  - electing a different leader if it is a leader for any partitions
            //  - removing it from the isr list if it's a replica for any paritions
            log.warn("Storage node '{}' has not sent a heartbeat in {} ms. Preparing to mark it as inactive.", node.getId(), expectIntervalMs);

            deactivate(node.getId());
        } else {
            if (node.isActive()) { return; }

            // TODO - more needs to be done here., for e.g.: ...
            log.info("Started receiving heartbeat from storage node '{}'. Preparing to mark it as active.", node.getId());
            activate(node.getId());
        }
    }

    public void checkStatus(MetadataSettings metadataSettings) {
        this.metadataSettings = metadataSettings;
        log.info("Storage node heartbeat monitor fired. Checking...");
        UnmanagedState.state.get(EntityType.STORAGE_NODE)
                .values()
                .stream()
                .map(StorageNode.class::cast)
                .map(StorageNode::getId)
                .forEach(this::checkStatus);
    }

    private void activate(String nodeId) {
        StorageNodeActivateRequest request = new StorageNodeActivateRequest(nodeId);
        try (RaftClient client = LocalClientBuilder.createRaftClient(metadataSettings)) {
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() != HttpStatus.OK.value()) {
                    log.error(response.getMessage());
                }
            } else {
                log.error("Error activating node '{}'.", nodeId, reply.getException());
            }
        } catch (Exception e) {
            log.error("Error activating node '{}'.", nodeId, e);
        }
    }

    private void deactivate(String nodeId) {
        StorageNodeDeactivateRequest request = new StorageNodeDeactivateRequest(nodeId);
        try (RaftClient client = LocalClientBuilder.createRaftClient(metadataSettings)) {
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() != HttpStatus.OK.value()) {
                    log.error(response.getMessage());
                }
            } else {
                log.error("Error deactivating node '{}'.", nodeId, reply.getException());
            }
        } catch (Exception e) {
            log.error("Error deactivating node '{}'.", nodeId, e);
        }
    }
}
