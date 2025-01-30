package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.BatchWrite;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class Writer {
    private String endpoints;

    protected void setEndpoints(String endpoints) {
        this.endpoints = endpoints;
    }

    void put(ItemPutRequest itemPutRequest) {
        for (String endpoint : endpoints.split(",")) {
            try {
                put(endpoint, itemPutRequest);
                return;
            } catch (Exception e) {
                log.trace(e.getMessage());
            }
        }

        throw new RuntimeException("Unable to write to any endpoint.");
    }

    private void put(String endpoint, ItemPutRequest request) {
        ItemPutResponse response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/put/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemPutResponse.class)
                .getBody();

        if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
            put(response.getLeaderEndpoint(), request);
            return;
        }

        if (response.getHttpStatusCode() == HttpStatus.OK.value())
            return;

        throw new RuntimeException("Unable to write to endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatusCode(), response.getErrorMessage()));
    }
}
