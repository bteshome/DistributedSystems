package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.storage.State;
import com.bteshome.keyvaluestore.storage.requests.DataWriteRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/data")
@RequiredArgsConstructor
@Slf4j
public class DataController {
    @Autowired
    State state;

    @GetMapping("/")
    public ResponseEntity<?> getItem(@RequestParam String table, String key) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(errorMessage);
        }

        if (!MetadataCache.getInstance().tableExists(table)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Table '%s' does not exist.".formatted(table));
        }

        int partition = KeyToPartitionMapper.map(table, key);
        return state.getItem(table, partition, key);
    }

    @PostMapping("/")
    public ResponseEntity<?> putItem(@RequestBody DataWriteRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(errorMessage);
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            String errorMessage = "Table '%s' does not exist.".formatted(request.getTable());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorMessage);
        }

        int partition = KeyToPartitionMapper.map(request.getTable(), request.getKey());
        return state.putItem(request.getTable(), partition, request.getKey(), request.getValue());
    }
}