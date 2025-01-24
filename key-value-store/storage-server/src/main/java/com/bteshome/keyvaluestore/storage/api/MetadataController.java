package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.ClientMetadataRefresher;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.NewLeaderElectedRequest;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/metadata")
@RequiredArgsConstructor
@Slf4j
public class MetadataController {
    @Autowired
    private ClientMetadataRefresher metadataRefresher;

    @Autowired
    StorageSettings storageSettings;

    @Autowired
    State state;

    @PostMapping("/table-created/")
    public ResponseEntity<?> fetch(@RequestBody Table table) {
        metadataRefresher.fetch();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/new-leader-elected/")
    public ResponseEntity<?> fetch(@RequestBody NewLeaderElectedRequest request) {
        state.newLeaderElected(request);
        return ResponseEntity.ok().build();
    }
}