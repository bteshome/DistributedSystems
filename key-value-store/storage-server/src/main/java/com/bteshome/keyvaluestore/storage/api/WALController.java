package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.states.State;
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
@RequestMapping("/api/wal")
@RequiredArgsConstructor
@Slf4j
public class WALController {
    @Autowired
    State state;

    @PostMapping("/fetch/")
    public ResponseEntity<WALFetchResponse> fetch(@RequestBody WALFetchRequest request) {
        if (!state.getLastHeartbeatSucceeded()) {
            String errorMessage = "Node '%s' is not active.".formatted(state.getNodeId());
            log.warn("{} from replica '{}' failed. {}", "WAL_FETCH", request.getId(), errorMessage);
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.SERVICE_UNAVAILABLE.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        if (!MetadataCache.getInstance().tableExists(request.getTable())) {
            String errorMessage = "Table '%s' does not exist.".formatted(request.getTable());
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        int maxNumRecordsAllowed = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_MAX_NUM_RECORDS_KEY);
        if (request.getMaxNumRecords() > maxNumRecordsAllowed) {
            String errorMessage = "Max number of records allowed is %d.".formatted(maxNumRecordsAllowed);
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.BAD_REQUEST.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        return state.fetch(
                request.getTable(),
                request.getPartition(),
                request.getLastFetchedEndOffset(),
                request.getLastFetchedLeaderTerm(),
                request.getMaxNumRecords(),
                request.getId());
    }
}