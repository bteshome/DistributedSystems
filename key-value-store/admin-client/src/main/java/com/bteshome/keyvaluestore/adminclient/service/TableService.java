package com.bteshome.keyvaluestore.adminclient.service;

import com.bteshome.keyvaluestore.adminclient.common.AdminClientException;
import com.bteshome.keyvaluestore.adminclient.common.Client;
import com.bteshome.keyvaluestore.adminclient.common.MetadataSettings;
import com.bteshome.keyvaluestore.adminclient.message.TableCreateRequest;
import com.bteshome.keyvaluestore.adminclient.message.TableGetRequest;
import com.bteshome.keyvaluestore.adminclient.dto.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TableService {
    @Autowired
    Client client;

    public void createTable(Table table){
        try (RaftClient client = this.client.createRaftClient()) {
            TableCreateRequest message = new TableCreateRequest(table);
            final RaftClientReply reply = client.io().send(message);
            if (reply.isSuccess()) {
                log.info("Table created successfully " + table.getName());
            } else {
                log.error("Error creating table: ", reply.getException());
                throw new AdminClientException(reply.getException());
            }
        } catch (Exception e) {
            log.error("Error occurred: ", e);
            throw new AdminClientException(e);
        }
    }

    public Table getTable(String tableName) {
        try (RaftClient client = this.client.createRaftClient()) {
            TableGetRequest message = new TableGetRequest(tableName);
            final RaftClientReply reply = client.io().sendReadOnly(message);
            if (reply.isSuccess()) {
                return Table.toTable(reply.getMessage().getContent());
            } else {
                throw new AdminClientException(reply.getException());
            }
        } catch (Exception e) {
            log.error("Error occurred: ", e);
            throw new AdminClientException(e);
        }
    }

    /*public Table listTables() {
        try (RaftClient client = client.createRaftClient()) {
            TableGetRequest message = TableGetRequest.toMessage(tableName);
            final RaftClientReply reply = client.io().sendReadOnly(message);
            if (reply.isSuccess()) {
                return Table.toTable(reply.getMessage().getContent());
            } else {
                throw new AdminClientException(reply.getException());
            }
        } catch (Exception e) {
            log.error("Error occurred: ", e);
            throw new AdminClientException(e);
        }
    }*/
}
