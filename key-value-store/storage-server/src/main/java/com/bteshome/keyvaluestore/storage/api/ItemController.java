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

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/items")
@RequiredArgsConstructor
@Slf4j
public class ItemController {
    @Autowired
    State state;

    @PostMapping("/get/")
    public ResponseEntity<ItemGetResponse> getItem(@RequestBody ItemGetRequest request) {
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
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build());
        }

        return partitionState.listItems(request.getLimit());
    }

    @PostMapping("/put/")
    public CompletableFuture<ResponseEntity<ItemPutResponse>> putItem(@RequestBody ItemPutRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return CompletableFuture.completedFuture(ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.putItems(request);
    }

    @PostMapping("/delete/")
    public CompletableFuture<ResponseEntity<ItemDeleteResponse>> deleteItem(@RequestBody ItemDeleteRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return CompletableFuture.completedFuture(ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.deleteItems(request);
    }

    @PostMapping("/count-and-offsets/")
    public ResponseEntity<ItemCountAndOffsetsResponse> countItems(@RequestBody ItemCountAndOffsetsRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build());
        }

        return partitionState.countItems();
    }
}