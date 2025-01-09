package com.bteshome.keyvaluestore.adminclient.service;

import com.bteshome.keyvaluestore.adminclient.common.AdminClientException;

import com.bteshome.keyvaluestore.common.ClientBuilder;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.requests.TableCreateRequest;
import com.bteshome.keyvaluestore.common.requests.TableGetRequest;
import com.bteshome.keyvaluestore.common.requests.TableListRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.common.responses.TableGetResponse;
import com.bteshome.keyvaluestore.common.responses.TableListResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class TableService {
    @Autowired
    ClientBuilder clientBuilder;

    public ResponseEntity<String> createTable(TableCreateRequest request) {
        request.Validate();

        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                return ResponseEntity.status(response.getHttpStatusCode()).body(response.getMessage());
            } else {
                log.error("Error creating table: ", reply.getException());
                throw new AdminClientException(reply.getException());
            }
        } catch (Exception e) {
            log.error("Error creating table: ", e);
            throw new AdminClientException(e);
        }
    }

    public ResponseEntity<?> getTable(TableGetRequest request) {
        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    TableGetResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return ResponseEntity.ok(response.getTableCopy());
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

    public ResponseEntity<?> list(TableListRequest request) {
        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    TableListResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return ResponseEntity.ok(response.getTableListCopy());
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
