package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.BatchWrite;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.requests.ItemDeleteRequest;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemDeleteResponse;
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
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class Writer {
    void put(ItemPutRequest itemPutRequest, int partition) {
        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(itemPutRequest.getTable(), partition);
        int retries = 0;

        ItemPutResponse response = put(endpoint, itemPutRequest);

        while (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value() && retries < 3) {
            retries++;
            response = put(response.getLeaderEndpoint(), itemPutRequest);
        }

        if (response.getHttpStatusCode() != HttpStatus.ACCEPTED.value()) {
            throw new RuntimeException("Unable to write to endpoint %s. Http status: %s, error: %s.".formatted(
                    endpoint,
                    response.getHttpStatusCode(),
                    response.getErrorMessage()));
        }
    }

    ItemPutResponse put(String endpoint, ItemPutRequest request) {
        return  RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/put/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemPutResponse.class)
                .getBody();
    }

    void delete(ItemDeleteRequest itemDeleteRequest, int partition) {
        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(itemDeleteRequest.getTable(), partition);
        int retries = 0;

        ItemDeleteResponse response = delete(endpoint, itemDeleteRequest);

        while (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value() && retries < 3) {
            retries++;
            response = delete(response.getLeaderEndpoint(), itemDeleteRequest);
        }

        if (response.getHttpStatusCode() != HttpStatus.ACCEPTED.value()) {
            throw new RuntimeException("Unable to delete at endpoint %s. Http status: %s, error: %s.".formatted(
                    endpoint,
                    response.getHttpStatusCode(),
                    response.getErrorMessage()));
        }
    }

    ItemDeleteResponse delete(String endpoint, ItemDeleteRequest request) {
        return RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/delete/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemDeleteResponse.class)
                .getBody();
    }
}
