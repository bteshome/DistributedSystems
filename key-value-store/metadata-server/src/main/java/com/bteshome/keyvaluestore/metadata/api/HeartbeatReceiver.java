package com.bteshome.keyvaluestore.metadata.api;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.requests.*;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.common.responses.StorageNodeHeartbeatResponse;
import com.bteshome.keyvaluestore.metadata.LocalClientBuilder;
import com.bteshome.keyvaluestore.metadata.MetadataSettings;
import com.bteshome.keyvaluestore.metadata.UnmanagedState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/api/heartbeat")
@RequiredArgsConstructor
@Slf4j
public class HeartbeatReceiver {
    @Autowired
    MetadataSettings metadataSettings;

    @PostMapping("/")
    public ResponseEntity<?> receive(@RequestBody StorageNodeHeartbeatRequest request) {
        if (!UnmanagedState.isLeader()) {
            String errorMessage = "Not leader. Refresh metadata.";
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(errorMessage);
        }

        if (!UnmanagedState.getStorageNodeIds().contains(request.getId())) {
            String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
            log.warn("{} failed. {}.", RequestType.STORAGE_NODE_HEARTBEAT, errorMessage);
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorMessage);
        }

        long currentMetadataVersion = UnmanagedState.getVersion();
        long threshold = (Long)UnmanagedState.getConfiguration(ConfigKeys.STORAGE_NODE_METADATA_LAG_MS_KEY);
        boolean isLaggingOnMetadata = request.getLastFetchedMetadataVersion() < currentMetadataVersion;
        boolean isLaggingOnMetadataBeyondThreshold = request.getLastFetchedMetadataVersion() < (currentMetadataVersion - threshold);

        log.debug("{}. Node: '{}', node metadata version: {}, current version: {}.",
                RequestType.STORAGE_NODE_HEARTBEAT,
                request.getId(),
                request.getLastFetchedMetadataVersion(),
                currentMetadataVersion);

        if (isLaggingOnMetadataBeyondThreshold) {
            log.warn("Node '{}' is lagging on metadata beyond the threshold '{}'. Preparing to mark it as inactive.",
                    request.getId(),
                    threshold
            );
            StorageNodeDeactivateRequest deactivateRequest = new StorageNodeDeactivateRequest(request.getId());
            try (RaftClient client = LocalClientBuilder.createRaftClient(metadataSettings)) {
                final RaftClientReply reply = client.io().send(deactivateRequest);
                if (reply.isSuccess()) {
                    String deactivateMessageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                    GenericResponse response = ResponseStatus.toGenericResponse(deactivateMessageString);
                    if (response.getHttpStatusCode() != HttpStatus.OK.value()) {
                        log.error(response.getMessage());
                    }
                } else {
                    log.error("Error deactivating node '{}'.", request.getId(), reply.getException());
                }
            } catch (Exception e) {
                log.error("Error deactivating node '{}'.", request.getId(), e);
            }
        }

        UnmanagedState.setHeartbeat(request.getId(), System.nanoTime());
        return ResponseEntity.ok(new StorageNodeHeartbeatResponse(isLaggingOnMetadata));
    }
}