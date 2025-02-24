package com.bteshome.keyvaluestore.admindashboard.service;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataClientBuilder;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.entities.RaftPeerInfo;
import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.requests.MetadataNodeListRequest;
import com.bteshome.keyvaluestore.common.requests.StorageNodeGetRequest;
import com.bteshome.keyvaluestore.common.requests.StorageNodeLeaveRequest;
import com.bteshome.keyvaluestore.common.requests.StorageNodeListRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.common.responses.MetadataNodeListResponse;
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
public class MetadataNodeService {
    @Autowired
    MetadataClientBuilder metadataClientBuilder;

    public List<RaftPeerInfo> list() {
        try (RaftClient client = this.metadataClientBuilder.createRaftClient()) {
            MetadataNodeListRequest request = new MetadataNodeListRequest();
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    MetadataNodeListResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return response.getMetadataNodes();
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
