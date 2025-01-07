package com.bteshome.keyvaluestore.adminclient.service;

import com.bteshome.keyvaluestore.adminclient.common.AdminClientException;
import com.bteshome.keyvaluestore.common.Client;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.requests.StorageNodeGetRequest;
import com.bteshome.keyvaluestore.common.requests.StorageNodeListRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.common.responses.StorageNodeGetResponse;
import com.bteshome.keyvaluestore.common.responses.StorageNodeListResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class NodeService {
    @Autowired
    Client client;

    public ResponseEntity<?> getNode(StorageNodeGetRequest request) {
        try (RaftClient client = this.client.createRaftClient()) {
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == ResponseStatus.OK) {
                    StorageNodeGetResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return ResponseEntity.ok(response.getStorageNodeCopy());
                }
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                return ResponseEntity.status(response.getHttpStatusCode()).body(response.getMessage());
            } else {
                throw new AdminClientException(reply.getException());
            }
        } catch (Exception e) {
            throw new AdminClientException(e);
        }
    }

    public ResponseEntity<?> list(StorageNodeListRequest request) {
        try (RaftClient client = this.client.createRaftClient()) {
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == ResponseStatus.OK) {
                    StorageNodeListResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return ResponseEntity.ok(response.getStorageNodeListCopy());
                }
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                return ResponseEntity.status(response.getHttpStatusCode()).body(response.getMessage());
            } else {
                throw new AdminClientException(reply.getException());
            }
        } catch (Exception e) {
            throw new AdminClientException(e);
        }
    }
}
