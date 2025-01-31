package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.client.requests.*;
import com.bteshome.keyvaluestore.client.responses.*;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
                    .httpStatusCode(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            return ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .errorMessage("Table '%s' does not exist.".formatted(request.getTable()))
                    .build());
        }

        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build());
        }

        return partitionState.getItem(request.getKey());
    }

    @PostMapping("/list/")
    public ResponseEntity<ItemListResponse> listItems(@RequestBody ItemListRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .errorMessage("Table '%s' does not exist.".formatted(request.getTable()))
                    .build());
        }

        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build());
        }

        return partitionState.listItems(request.getLimit());
    }

    @PostMapping("/put/")
    public ResponseEntity<ItemPutResponse> putItem(@RequestBody ItemPutRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            String errorMessage = "Table '%s' does not exist.".formatted(request.getTable());
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        return partitionState.putItems(request.getItems());
    }

    @PostMapping("/delete/")
    public ResponseEntity<ItemDeleteResponse> deleteItem(@RequestBody ItemDeleteRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            String errorMessage = "Table '%s' does not exist.".formatted(request.getTable());
            return ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        return partitionState.deleteItems(request.getKeys());
    }

    @PostMapping("/count-and-offsets/")
    public ResponseEntity<ItemCountAndOffsetsResponse> countItems(@RequestBody ItemCountAndOffsetsRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .errorMessage("Table '%s' does not exist.".formatted(request.getTable()))
                    .build());
        }

        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build());
        }

        return partitionState.countItems();
    }
}