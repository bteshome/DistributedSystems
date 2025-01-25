package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.MetadataClientBuilder;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.common.requests.ReplicaAddToISRRequest;
import com.bteshome.keyvaluestore.common.requests.ReplicaRemoveFromISRRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Set;

@Component
@Slf4j
public class ISRSynchronizer {
    @Autowired
    MetadataClientBuilder metadataClientBuilder;

    public void removeFromInSyncReplicaLists(Set<Replica> laggingReplicas) {
        ReplicaRemoveFromISRRequest request = new ReplicaRemoveFromISRRequest(laggingReplicas);

        try (RaftClient client = this.metadataClientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() != HttpStatus.OK.value())
                    log.error("Error removing replicas from ISR lists: {}", response.getMessage());
                else
                    log.info("Removed replicas from ISR lists: {}", laggingReplicas);
            } else {
                log.error("Error removing replicas from ISR lists:", reply.getException());
            }
        } catch (Exception e) {
            log.error("Error removing replicas from ISR lists: ", e);
        }
    }

    public void addToInSyncReplicaLists(Set<Replica> caughtUpReplicas) {
        ReplicaAddToISRRequest request = new ReplicaAddToISRRequest(caughtUpReplicas);

        try (RaftClient client = this.metadataClientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() != HttpStatus.OK.value())
                    log.error("Error adding replicas to ISR lists: {}", response.getMessage());
                else
                    log.info("Added replicas to ISR lists: {}", caughtUpReplicas);
            } else {
                log.error("Error adding replicas to ISR lists:", reply.getException());
            }
        } catch (Exception e) {
            log.error("Error adding replicas to ISR lists: ", e);
        }
    }
}
