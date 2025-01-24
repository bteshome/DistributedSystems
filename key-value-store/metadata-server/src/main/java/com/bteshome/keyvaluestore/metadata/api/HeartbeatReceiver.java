package com.bteshome.keyvaluestore.metadata.api;

import com.bteshome.keyvaluestore.common.requests.*;
import com.bteshome.keyvaluestore.common.responses.StorageNodeHeartbeatResponse;
import com.bteshome.keyvaluestore.metadata.MetadataSettings;
import com.bteshome.keyvaluestore.metadata.UnmanagedState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/heartbeat")
@RequiredArgsConstructor
@Slf4j
public class HeartbeatReceiver {
    @Autowired
    MetadataSettings metadataSettings;

    @PostMapping("/")
    public ResponseEntity<StorageNodeHeartbeatResponse> receive(@RequestBody StorageNodeHeartbeatRequest request) {
        if (!UnmanagedState.getInstance().isLeader()) {
            return ResponseEntity.ok(StorageNodeHeartbeatResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .errorMessage("Not leader. Refresh metadata.")
                    .build());
        }

        if (!UnmanagedState.getInstance().getStorageNodeIds().contains(request.getId())) {
            String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
            log.warn("{} failed. {}.", MetadataRequestType.STORAGE_NODE_HEARTBEAT, errorMessage);
            return ResponseEntity.ok(StorageNodeHeartbeatResponse.builder()
                    .httpStatusCode(HttpStatus.UNAUTHORIZED.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        long currentMetadataVersion = UnmanagedState.getInstance().getVersion();
        boolean isLaggingOnMetadata = request.getLastFetchedMetadataVersion() < currentMetadataVersion;

        log.debug("{}. Node: '{}', node metadata version: {}, current version: {}.",
                MetadataRequestType.STORAGE_NODE_HEARTBEAT,
                request.getId(),
                request.getLastFetchedMetadataVersion(),
                currentMetadataVersion);

        UnmanagedState.getInstance().setHeartbeatTime(request.getId(), System.nanoTime());
        return ResponseEntity.ok(StorageNodeHeartbeatResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .laggingOnMetadata(isLaggingOnMetadata)
                .build());
    }
}