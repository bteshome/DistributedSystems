package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.common.ClientBuilder;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.common.requests.ReplicaRemoveFromISRRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ReplicaMonitor {
    private ScheduledExecutorService executor = null;

    @Autowired
    ClientBuilder clientBuilder;

    @Autowired
    State state;

    @PreDestroy
    public void close() {
        if (executor != null) {
            executor.close();
        }
    }

    public void schedule() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.STORAGE_NODE_REPLICA_MONITOR_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::checkStatus, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled replica monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replica monitor.", e);
        }
    }

    private void checkStatus() {
        //log.info("Replica monitor fired. Checking...");

        /*try {
            String leaderNodeId = replicationState.getId();
            List<Replica> ownedReplicas = MetadataCache.getInstance().getOwnedReplicas(leaderNodeId);
            Set<Replica> laggingReplicas = new HashSet<>();
            long threshold = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.STORAGE_NODE_REPLICA_LAG_THRESHOLD_KEY);

            for (Replica ownedReplica : ownedReplicas) {
                List<String> allReplicaNodeIds = MetadataCache.getInstance().getReplicaNodeIds(ownedReplica.getTable(), ownedReplica.getPartition());
                long leaderOffset = states.getOffset(ownedReplica.getTable(), ownedReplica.getPartition(), leaderNodeId);

                for (String replicaId : allReplicaNodeIds) {
                    if (replicaId.equals(leaderNodeId)) {
                        continue;
                    }
                    long replicaOffset = replicationState.getOffset(ownedReplica.getTable(), ownedReplica.getPartition(), replicaId);
                    if (leaderOffset - replicaOffset > threshold) {
                        laggingReplicas.add(new Replica(replicaId, ownedReplica.getTable(), ownedReplica.getPartition()));
                    }
                }
            }

            if (laggingReplicas.isEmpty()) {
                return;
            }

            for (Replica laggingReplica : laggingReplicas) {
                log.warn("Some replicas are lagging on data fetch beyond the threshold. Preparing to remove them from ISR lists: {}", laggingReplica);
            }

            removeFromISRs(laggingReplicas);
        } catch (Exception e) {
            log.error("Error checking replicas", e);
        }*/
    }

    private void addToISR() {
        // TODO
    }

    private void removeFromISRs(Set<Replica> laggingReplicas) {
        ReplicaRemoveFromISRRequest request = new ReplicaRemoveFromISRRequest(laggingReplicas);

        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() != HttpStatus.OK.value()) {
                    log.error("Error removing replicas from ISR lists: {}", response.getMessage());
                }
            } else {
                log.error("Error removing replicas from ISR lists:", reply.getException());
            }
        } catch (Exception e) {
            log.error("Error removing replicas from ISR lists: ", e);
        }
    }
}
