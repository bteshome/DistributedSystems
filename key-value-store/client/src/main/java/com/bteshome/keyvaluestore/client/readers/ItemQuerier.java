package com.bteshome.keyvaluestore.client.readers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.clientrequests.ItemQuery;
import com.bteshome.keyvaluestore.client.requests.ItemQueryRequest;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.common.JsonSerDe;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class ItemQuerier {
    @Autowired
    WebClient webClient;

    public Flux<Map.Entry<String, String>> queryForStrings(ItemQuery request) {
        return queryForBytes(request).map(item -> {
            String stringValue = new String(item.getValue());
            return Map.entry(item.getKey(), stringValue);
        });
    }

    public <T> Flux<Map.Entry<String, T>> queryForObjects(ItemQuery request, Class<T> clazz) {
        return queryForBytes(request).map(item -> {
            T valueTyped = JsonSerDe.deserialize(item.getValue(), clazz);
            return Map.entry(item.getKey(), valueTyped);
        });
    }

    public Flux<Map.Entry<String, byte[]>> queryForBytes(ItemQuery request) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(request.getTable());
        final Map<Integer, List<Map.Entry<String, String>>> result = new ConcurrentHashMap<>();

        HashMap<String, ItemQueryRequest> partitionRequests = new HashMap<>();

        for (int partition = 1; partition <= numPartitions; partition++) {
            final String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
            ItemQueryRequest itemQueryRequest = new ItemQueryRequest();
            itemQueryRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
            itemQueryRequest.setPartition(partition);
            itemQueryRequest.setIndexName(Validator.notEmpty(request.getIndexName(), "Index field"));
            itemQueryRequest.setIndexKey(Validator.notEmpty(request.getIndexKey(), "Index key"));
            itemQueryRequest.setIsolationLevel(request.getIsolationLevel());
            partitionRequests.put(endpoint, itemQueryRequest);
        }

        return Flux.fromIterable(partitionRequests.entrySet())
                .flatMap(partitionRequest -> query(
                        partitionRequest.getKey(),
                        partitionRequest.getValue()))
                .map(ItemListResponse::getItems)
                .flatMapIterable(r -> r);
    }

    private Mono<ItemListResponse> query(String endpoint, ItemQueryRequest request) {
        return webClient
                .post()
                .uri("http://%s/api/items/query/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .toEntity(ItemListResponse.class)
                .map(HttpEntity::getBody)
                .flatMap(response -> {
                    if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
                        return query(response.getLeaderEndpoint(), request);
                    } else if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                        return Mono.just(response);
                    } else {
                        return Mono.error(new ClientException("Unexpected status code: %s, %s".formatted(response.getHttpStatusCode(), response.getErrorMessage())));
                    }
                });
    }
}
