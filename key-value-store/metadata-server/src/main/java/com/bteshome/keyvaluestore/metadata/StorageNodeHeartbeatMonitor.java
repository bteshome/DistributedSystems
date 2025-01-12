package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.ResponseStatus;
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
        Long lastHeartbeatTime = UnmanagedState.getInstance().getHeartbeat(nodeId);

        if (lastHeartbeatTime == null) {
            return;
        }

        StorageNodeStatus nodeStatus = UnmanagedState.getInstance().getStorageNodeStatus(nodeId);
        long expectIntervalMs = (Long)UnmanagedState.getInstance().getConfiguration(ConfigKeys.STORAGE_NODE_HEARTBEAT_EXPECT_INTERVAL_MS_KEY);
        boolean isLaggingOnHeartbeats = lastHeartbeatTime < System.nanoTime() - 1000000L * expectIntervalMs;

        if (isLaggingOnHeartbeats) {
            if (nodeStatus == StorageNodeStatus.ACTIVE) {
                log.warn("Storage node '{}' has not sent a heartbeat in '{}' ms. Preparing to mark it as inactive.", nodeId, expectIntervalMs);
                deactivate(nodeId);
            }
            return;
        }

        if (nodeStatus == StorageNodeStatus.INACTIVE) {
            log.info("Started receiving heartbeat from storage node '{}'. Preparing to mark it as active.", nodeId);
            activate(nodeId);
        }
    }

    public void checkStatus(MetadataSettings metadataSettings) {
        this.metadataSettings = metadataSettings;
        log.info("Storage node heartbeat monitor fired. Checking...");
        UnmanagedState.getInstance().getStorageNodeIds().forEach(this::checkStatus);
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
