package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.Store;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.requests.DataWriteRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/data")
@RequiredArgsConstructor
@Slf4j
public class DataController {
    @Autowired
    Store store;

    @GetMapping("/")
    public ResponseEntity<?> read(@RequestParam String table, String key) {
        if (!MetadataCache.getInstance().tableExists(table)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Table '%s' does not exist.".formatted(table));
        }

        return store.get(table, key);
    }

    @PostMapping("/")
    public ResponseEntity<?> write(@RequestBody DataWriteRequest request) {
        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Table '%s' does not exist.".formatted(request.getTable()));
        }

        return store.put(request.getTable(), request.getKey(), request.getValue());
    }
}