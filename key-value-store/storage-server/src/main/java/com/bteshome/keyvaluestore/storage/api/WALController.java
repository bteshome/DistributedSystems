package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.State;
import com.bteshome.keyvaluestore.storage.requests.WALFetchAckRequest;
import com.bteshome.keyvaluestore.storage.requests.WALFetchRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/fetch")
@RequiredArgsConstructor
@Slf4j
public class WALController {
    @Autowired
    State state;

    @PostMapping("/")
    public ResponseEntity<?> fetch(@RequestBody WALFetchRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            log.warn("{} from replica '{}' failed. {}", "FETCH", request.getId(), errorMessage);
            return ResponseEntity.ok(new WALFetchResponse(HttpStatus.SERVICE_UNAVAILABLE, errorMessage));
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            String errorMessage = "Table '%s' does not exist.".formatted(request.getTable());
            return ResponseEntity.ok(new WALFetchResponse(HttpStatus.NOT_FOUND, errorMessage));
        }

        return state.fetch(request.getTable(), request.getPartition(), request.getLastFetchedOffset());
    }

    @PostMapping("/ack/")
    public ResponseEntity<?> ack(@RequestBody WALFetchAckRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            log.warn("{} from replica '{}' failed. {}", "FETCH_ACK", request.getId(), errorMessage);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(errorMessage);
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            String errorMessage = "Table '%s' does not exist.".formatted(request.getTable());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorMessage);
        }

        return state.ackFetch(request.getTable(), request.getPartition(), request.getEndOffset(), request.getId());
    }
}