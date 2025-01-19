package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.client.requests.ItemCountAndOffsetsRequest;
import com.bteshome.keyvaluestore.client.requests.ItemGetRequest;
import com.bteshome.keyvaluestore.client.requests.ItemListRequest;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.MetadataCache;
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

        return state.getItem(request.getTable(), request.getPartition(), request.getKey());
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

        return state.listItems(request.getTable(), request.getPartition(), request.getLimit());
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

        return state.putItem(request.getTable(), request.getPartition(), request.getKey(), request.getValue());
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

        return state.countItems(request.getTable(), request.getPartition());
    }
}