package com.bteshome.keyvaluestore.client.readers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.requests.ItemListRequest;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.common.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

@Component
@Slf4j
public class ItemLister {
    @Autowired
    WebClient webClient;
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public Flux<Tuple3<String, String, String>> listStrings(ItemList request) {
        return listBytes(request).map(item -> {
            String key = item.first();
            String partitionKey = item.second();
            byte[] value = item.third();
            String stringValue = new String(value);
            return Tuple3.of(key, partitionKey, stringValue);
        });
    }

    public <T> Flux<Tuple3<String, String, T>> listObjects(ItemList request, Class<T> clazz) {
        return listBytes(request).map(item -> {
            String key = item.first();
            String partitionKey = item.second();
            byte[] value = item.third();
            T valueTyped = JsonSerDe.deserialize(value, clazz);
            return Tuple3.of(key, partitionKey, valueTyped);
        });
    }

    public Flux<Tuple3<String, String, byte[]>> listBytes(ItemList request) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(request.getTable());
        final Map<Integer, List<Map.Entry<String, String>>> result = new ConcurrentHashMap<>();

        List<Tuple<String, ItemListRequest>> partitionRequests = new ArrayList<>();

        List<Integer> partitionsToFetchFrom;
        if (Strings.isBlank(request.getPartitionKey())) {
            partitionsToFetchFrom = IntStream.rangeClosed(1, numPartitions).boxed().toList();
        } else {
            int partition = keyToPartitionMapper.map(request.getTable(), request.getPartitionKey());
            partitionsToFetchFrom = Collections.singletonList(partition);
        }

        for (int partition : partitionsToFetchFrom) {
            final String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
            ItemListRequest itemListRequest = new ItemListRequest();
            itemListRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
            itemListRequest.setPartition(partition);
            itemListRequest.setLimit(request.getLimit());
            if (!Strings.isBlank(request.getPartitionKey()))
                itemListRequest.setLastReadItemKey(request.getLastReadItemKey());
            itemListRequest.setIsolationLevel(request.getIsolationLevel());
            partitionRequests.add(Tuple.of(endpoint, itemListRequest));
        }

        return Flux.fromIterable(partitionRequests)
                .flatMap(partitionRequest -> list(
                        partitionRequest.first(),
                        partitionRequest.second()))
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
