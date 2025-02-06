package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.ISRListChangedRequest;
import com.bteshome.keyvaluestore.common.requests.NewLeaderElectedRequest;
import com.bteshome.keyvaluestore.client.responses.ClientMetadataFetchResponse;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.core.StorageNodeMetadataRefresher;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/metadata")
@RequiredArgsConstructor
@Slf4j
public class MetadataController {
    @Autowired
    private StorageNodeMetadataRefresher metadataRefresher;

    @Autowired
    StorageSettings storageSettings;

    @Autowired
    State state;

    @PostMapping("/table-created/")
    public Mono<ResponseEntity<?>> tableCreated(@RequestBody Table table) {
        log.info("Received a TABLE CREATED notification from the metadata node for table '{}'.", table.getName());
        metadataRefresher.fetch();
        state.tableCreated(table);
        return Mono.just(ResponseEntity.ok().build());
    }

    @PostMapping("/new-leader-elected/")
    public Mono<ResponseEntity<?>> newLeaderElected(@RequestBody NewLeaderElectedRequest request) {
        log.info("Received a NewLeaderElected notification from the metadata node for table '{}' partition '{}'. " +
                        "New leader node id is: '{}'",
                request.getTableName(),
                request.getPartitionId(),
                request.getNewLeaderId());
        PartitionState partitionState = state.getPartitionState(request.getTableName(), request.getPartitionId());

        if (partitionState == null) {
            log.warn("Partition state not found for table '{}' partition '{}'.", request.getTableName(), request.getPartitionId());
        } else {
            partitionState.newLeaderElected(request);
            metadataRefresher.fetch();
        }

        return Mono.just(ResponseEntity.ok().build());
    }

    @PostMapping("/isr-list-changed/")
    public Mono<ResponseEntity<?>> isrListChanged(@RequestBody ISRListChangedRequest request) {
        log.info("Received an ISRListChanged notification from the metadata node for table '{}' partition '{}'.",
                request.getTableName(),
                request.getPartitionId());
        PartitionState partitionState = state.getPartitionState(request.getTableName(), request.getPartitionId());

        if (partitionState == null) {
            log.warn("Partition state not found for table '{}' partition '{}'.", request.getTableName(), request.getPartitionId());
        } else {
            partitionState.isrListChanged(request);
            metadataRefresher.fetch();
        }

        return Mono.just(ResponseEntity.ok().build());
    }

    @PostMapping("/get-metadata/")
    public Mono<ResponseEntity<?>> getMetadata() {
        return Mono.just(ResponseEntity.ok(ClientMetadataFetchResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .serializedMetadata(MetadataCache.getInstance().getState())
                .build()));
    }
}