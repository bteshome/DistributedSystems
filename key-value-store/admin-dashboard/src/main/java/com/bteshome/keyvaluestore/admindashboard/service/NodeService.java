package com.bteshome.keyvaluestore.admindashboard.service;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.common.ClientBuilder;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.requests.StorageNodeGetRequest;
import com.bteshome.keyvaluestore.common.requests.StorageNodeListRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.common.responses.StorageNodeGetResponse;
import com.bteshome.keyvaluestore.common.responses.StorageNodeListResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@Service
public class NodeService {
    @Autowired
    ClientBuilder clientBuilder;

    public StorageNode getNode(StorageNodeGetRequest request) {
        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    StorageNodeGetResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return response.getStorageNodeCopy();
                }
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value()) {
                    return null;
                }
                throw new AdminDashboardException(response.getMessage());
            } else {
                throw new AdminDashboardException(reply.getException());
            }
        } catch (Exception e) {
            throw new AdminDashboardException(e);
        }
    }

    public List<StorageNode> list(StorageNodeListRequest request) {
        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    StorageNodeListResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return response.getStorageNodeListCopy();
                }
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                throw new AdminDashboardException(response.getMessage());
            } else {
                throw new AdminDashboardException(reply.getException());
            }
        } catch (Exception e) {
            throw new AdminDashboardException(e);
        }
    }
}
