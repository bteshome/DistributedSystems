package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.ISRListChangedRequest;
import com.bteshome.keyvaluestore.common.requests.NewLeaderElectedRequest;
import com.bteshome.keyvaluestore.client.responses.ClientMetadataFetchResponse;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.core.StorageNodeMetadataRefresher;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
    public ResponseEntity<?> tableCreated(@RequestBody Table table) {
        metadataRefresher.fetch();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/new-leader-elected/")
    public ResponseEntity<?> newLeaderElected(@RequestBody NewLeaderElectedRequest request) {
        state.newLeaderElected(request);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/isr-list-changed/")
    public ResponseEntity<?> isrListChanged(@RequestBody ISRListChangedRequest request) {
        metadataRefresher.fetch();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/get-metadata/")
    public ResponseEntity<?> getMetadata() {
        return ResponseEntity.ok(ClientMetadataFetchResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .serializedMetadata(MetadataCache.getInstance().getState())
                .build());
    }
}