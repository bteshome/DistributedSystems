package com.bteshome.keyvaluestore.admindashboard.service;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.admindashboard.common.ConfigurationCache;

import com.bteshome.keyvaluestore.common.ClientBuilder;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.entities.Table;
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
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@Service
public class TableService {
    @Autowired
    ClientBuilder clientBuilder;

    @Autowired
    ConfigurationCache configurationCache;

    public void createTable(TableCreateRequest request) {
        request.validate(configurationCache.getConfigurations());

        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() != HttpStatus.OK.value()) {
                    throw new AdminDashboardException(response.getMessage());
                }
            } else {
                throw new AdminDashboardException(reply.getException());
            }
        } catch (Exception e) {
            throw new AdminDashboardException(e);
        }
    }

    public Table getTable(TableGetRequest request) {
        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    TableGetResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return response.getTableCopy();
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

    public List<Table> list(TableListRequest request) {
        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            final RaftClientReply reply = client.io().sendReadOnly(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    TableListResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    return response.getTableListCopy();
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
