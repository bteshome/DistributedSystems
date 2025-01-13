package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.ReplicationState;
import com.bteshome.keyvaluestore.storage.requests.ReplicaFetchRequest;
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
public class ReplicaFetchController {
    @Autowired
    ReplicationState replicationState;

    @PostMapping("/")
    public ResponseEntity<?> fetch(@RequestBody ReplicaFetchRequest request) {
        boolean lastHeartbeatSucceeded = replicationState.isLastHeartbeatSucceeded();
        String thisNodeId = replicationState.getId();

        if (!lastHeartbeatSucceeded) {
            String errorMessage = "Node '%s' is not active.".formatted(thisNodeId);
            log.warn("{} from replica '{}' failed. {}", "FETCH", request.getId(), errorMessage);
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(errorMessage);
        }

        if (!MetadataCache.getInstance().getLeaderNodeId(request.getTable(), request.getPartition()).equals(thisNodeId)) {
            String errorMessage = "Not the leader for table '{}' partition '{}'.".formatted(request.getTable(), request.getPartition());
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(errorMessage);
        }

        log.debug("{} received. Replica: '{}', last offset: {}, leader offset: {}.",
                "FETCH",
                request.getId(),
                request.getLastFetchedOffset(),
                replicationState.getOffset(request.getTable(), request.getPartition(), thisNodeId));

        // TODO - send the data
        return ResponseEntity.ok().build();
    }
}