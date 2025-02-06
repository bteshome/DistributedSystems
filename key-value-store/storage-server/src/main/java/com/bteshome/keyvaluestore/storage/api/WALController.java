package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.storage.requests.WALGetReplicaEndOffsetRequest;
import com.bteshome.keyvaluestore.storage.responses.WALGetReplicaEndOffsetResponse;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import com.bteshome.keyvaluestore.storage.requests.WALFetchRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping(value = "/api/wal", produces = {MediaType.APPLICATION_JSON_VALUE})
@RequiredArgsConstructor
@Slf4j
public class WALController {
    @Autowired
    State state;

    @PostMapping("/fetch/")
    public Mono<ResponseEntity<WALFetchResponse>> fetch(@RequestBody WALFetchRequest request) {
        try {
            PartitionState partitionState = state.getPartitionState(request.getTable(),
                                                                    request.getPartition());

             if (partitionState == null) {
                return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build()));
            }

            return partitionState.getLogEntries(request.getLastFetchOffset(),
                                                request.getMaxNumRecords(),
                                                request.getId());
        } catch (Exception e) {
            return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build()));
        }
    }

    @PostMapping("/get-end-offset/")
    public Mono<ResponseEntity<WALGetReplicaEndOffsetResponse>> fetch(@RequestBody WALGetReplicaEndOffsetRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(),
                                                                request.getPartition());

        if (partitionState == null) {
            return Mono.just(ResponseEntity.ok(WALGetReplicaEndOffsetResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return Mono.just(ResponseEntity.ok(WALGetReplicaEndOffsetResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .endOffset(partitionState.getOffsetState().getEndOffset())
                .build()));
    }
}