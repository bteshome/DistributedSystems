package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.client.*;
import com.bteshome.keyvaluestore.storage.MetadataCache;
import com.bteshome.keyvaluestore.storage.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/items")
@RequiredArgsConstructor
@Slf4j
public class ItemController {
    @Autowired
    State state;

    @PostMapping("/get/")
    public ResponseEntity<ItemGetResponse> getItem(@RequestBody ItemGetRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatus(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            return ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatus(HttpStatus.NOT_FOUND.value())
                    .errorMessage("Table '%s' does not exist.".formatted(request.getTable()))
                    .build());
        }

        int partition = KeyToPartitionMapper.map(request.getTable(), request.getKey());
        return state.getItem(request.getTable(), partition, request.getKey());
    }

    @PostMapping("/list/")
    public ResponseEntity<ItemListResponse> listItems(@RequestBody ItemListRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatus(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatus(HttpStatus.NOT_FOUND.value())
                    .errorMessage("Table '%s' does not exist.".formatted(request.getTable()))
                    .build());
        }

        return state.listItems(request.getTable(), request.getPartition(), request.getLimit());
    }

    @PostMapping("/put/")
    public ResponseEntity<ItemPutResponse> putItem(@RequestBody ItemPutRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatus(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            String errorMessage = "Table '%s' does not exist.".formatted(request.getTable());
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatus(HttpStatus.NOT_FOUND.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        int partition = KeyToPartitionMapper.map(request.getTable(), request.getKey());
        return state.putItem(request.getTable(), partition, request.getKey(), request.getValue());
    }

    @PostMapping("/count/")
    public ResponseEntity<ItemCountResponse> countItems(@RequestBody ItemCountRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.ok(ItemCountResponse.builder()
                    .httpStatus(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            return ResponseEntity.ok(ItemCountResponse.builder()
                    .httpStatus(HttpStatus.NOT_FOUND.value())
                    .errorMessage("Table '%s' does not exist.".formatted(request.getTable()))
                    .build());
        }

        return state.countItems(request.getTable(), request.getPartition());
    }
}