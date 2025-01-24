package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.ClientMetadataRefresher;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.NewLeaderElectedRequest;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

import java.util.List;

@RestController
@RequestMapping("/api/metadata")
@RequiredArgsConstructor
@Slf4j
public class MetadataController {
    @Autowired
    private ClientMetadataRefresher metadataRefresher;

    @Autowired
    StorageSettings storageSettings;

    @Autowired
    State state;

    @PostMapping("/table-created/")
    public ResponseEntity<?> fetch(@RequestBody Table table) {
        metadataRefresher.fetch();
        return ResponseEntity.ok().build();
    }

    // TODO - offset synchronization & truncation
    @PostMapping("/new-leader-elected/")
    public ResponseEntity<?> fetch(@RequestBody NewLeaderElectedRequest request) {
        /*if (storageSettings.getNode().getId().equals(request.getNewLeaderId())) {
            log.info("This node elected as the new leader for table '{}' partition '{}'.",
                    request.getTableName(),
                    request.getPartitionId());
            long committedOffsetFromPreviousLeader = state.getCommittedOffsetReadWriter().read(request.getTableName(), request.getPartitionId());
            long committedOffsetThisNodeReplicated = state.getCommittedOffset(request.getTableName(), request.getPartitionId());
            if (committedOffsetThisNodeReplicated < committedOffsetFromPreviousLeader) {

            }

            List<String> replicaEndpoints = MetadataCache.getInstance().getReplicaEndpoints(request.getTableName(), request.getPartitionId());

            var response = RestClient.builder()
                    .build()
                    .post()
                    .uri("http://%s/api/wal/get-committed-offset/".formatted(leaderEndpoint))
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .toEntity(WALFetchResponse.class)
                    .getBody();
        }*/

        //metadataRefresher.fetch();

        return ResponseEntity.ok().build();
    }
}