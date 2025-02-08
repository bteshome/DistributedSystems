package com.bteshome.keyvaluestore.client.readers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.requests.ItemListRequest;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class BatchReader {
    @Autowired
    WebClient webClient;

    public Flux<Map.Entry<String, String>> listStrings(ItemList request) {
        return list(request).doOnNext(item -> {
            byte[] bytes = Base64.getDecoder().decode(item.getValue());
            item.setValue(new String(bytes));
        });
    }

    public <T> Flux<Map.Entry<String, T>> listObjects(ItemList request) {
        return list(request).map(item -> {
            T valueTyped = JavaSerDe.deserialize(item.getValue());
            return Map.entry(item.getKey(), valueTyped);
        });
    }

    public Flux<Map.Entry<String, byte[]>> listBytes(ItemList request) {
        return list(request).map(item -> {
            byte[] bytes = Base64.getDecoder().decode(item.getValue());
            return Map.entry(item.getKey(), bytes);
        });
    }

    private Flux<Map.Entry<String, String>> list(ItemList request) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(request.getTable());
        final Map<Integer, List<Map.Entry<String, String>>> result = new ConcurrentHashMap<>();

        HashMap<String, ItemListRequest> partitionRequests = new HashMap<>();

        for (int partition = 1; partition <= numPartitions; partition++) {
            final String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
            ItemListRequest itemListRequest = new ItemListRequest();
            itemListRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
            itemListRequest.setPartition(partition);
            itemListRequest.setLimit(request.getLimit());
            partitionRequests.put(endpoint, itemListRequest);
        }

        return Flux.fromIterable(partitionRequests.entrySet())
                .flatMap(partitionRequest -> list(
                        partitionRequest.getKey(),
                        partitionRequest.getValue()))
                .map(ItemListResponse::getItems)
                .flatMapIterable(r -> r);
    }

    private Mono<ItemListResponse> list(String endpoint, ItemListRequest request) {
        return webClient
                .post()
                .uri("http://%s/api/items/list/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .toEntity(ItemListResponse.class)
                .map(HttpEntity::getBody)
                .flatMap(response -> {
                    if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
                        return list(response.getLeaderEndpoint(), request);
                    } else if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                        return Mono.just(response);
                    } else {
                        return Mono.error(new ClientException("Unexpected status code: %s, %s".formatted(response.getHttpStatusCode(), response.getErrorMessage())));
                    }
                });
    }
}
