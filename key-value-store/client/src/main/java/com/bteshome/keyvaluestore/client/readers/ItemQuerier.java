package com.bteshome.keyvaluestore.client.readers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.clientrequests.ItemQuery;
import com.bteshome.keyvaluestore.client.requests.ItemQueryRequest;
import com.bteshome.keyvaluestore.client.responses.CursorPosition;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.client.responses.ItemListResponseFlattened;
import com.bteshome.keyvaluestore.client.responses.ItemResponse;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

@Component
@Slf4j
public class ItemQuerier {
    @Autowired
    WebClient webClient;
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public Mono<ItemListResponseFlattened<String>> queryForStrings(ItemQuery request) {
        return queryForBytes(request).map(response -> {
            Map<Integer, CursorPosition> cursorPositions = response.getCursorPositions();
            List<ItemResponse<String>> items = response.getItems().stream().map(item -> {
                String key = item.getItemKey();
                String partitionKey = item.getPartitionKey();
                byte[] value = item.getValue();
                String stringValue = new String(value);
                return new ItemResponse<>(key, partitionKey, stringValue);
            }).toList();
            return new ItemListResponseFlattened<>(items, cursorPositions);
        });
    }

    public <T> Mono<ItemListResponseFlattened<T>> queryForObjects(ItemQuery request, Class<T> clazz) {
        return queryForBytes(request).map(response -> {
            Map<Integer, CursorPosition> cursorPositions = response.getCursorPositions();
            List<ItemResponse<T>> items = response.getItems().stream().map(item -> {
                String key = item.getItemKey();
                String partitionKey = item.getPartitionKey();
                byte[] value = item.getValue();
                T valueTyped = JsonSerDe.deserialize(value, clazz);
                return new ItemResponse<>(key, partitionKey, valueTyped);
            }).toList();
            return new ItemListResponseFlattened<>(items, cursorPositions);
        });
    }

    public Mono<ItemListResponseFlattened<byte[]>> queryForBytes(ItemQuery request) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(request.getTable());
        final Map<Integer, List<Map.Entry<String, String>>> result = new ConcurrentHashMap<>();

        List<Tuple<String, ItemQueryRequest>> partitionRequests = new ArrayList<>();

        List<Integer> partitionsToFetchFrom;
        if (Strings.isBlank(request.getPartitionKey())) {
            partitionsToFetchFrom = IntStream.rangeClosed(1, numPartitions).boxed().toList();
        } else {
            int partition = keyToPartitionMapper.map(request.getTable(), request.getPartitionKey());
            partitionsToFetchFrom = Collections.singletonList(partition);
        }

        for (int partition : partitionsToFetchFrom) {
            ItemQueryRequest itemQueryRequest = new ItemQueryRequest();

            if (request.getCursorPositions() != null && request.getCursorPositions().containsKey(partition)) {
                CursorPosition cursorPosition = request.getCursorPositions().get(partition);
                if (!cursorPosition.isHasMore())
                    continue;
                itemQueryRequest.setLastReadItemKey(cursorPosition.getLastReadItemKey());
            }

            final String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
            itemQueryRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
            itemQueryRequest.setPartition(partition);
            itemQueryRequest.setIndexName(Validator.notEmpty(request.getIndexName(), "Index field"));
            itemQueryRequest.setIndexKey(Validator.notEmpty(request.getIndexKey(), "Index key"));
            itemQueryRequest.setLimit(request.getLimit());
            itemQueryRequest.setIsolationLevel(request.getIsolationLevel());
            partitionRequests.add(Tuple.of(endpoint, itemQueryRequest));
        }

        return Flux.fromIterable(partitionRequests)
                .flatMap(partitionRequest -> query(
                        partitionRequest.first(),
                        partitionRequest.second()))
                .collectList()
                .map(r -> {
                    List<ItemResponse<byte[]>> items = new ArrayList<>();
                    Map<Integer, CursorPosition> cursorPositions = new HashMap<>();
                    for (ItemListResponse response : r) {
                        items.addAll(response.getItems());
                        if (response.getCursorPosition() != null)
                            cursorPositions.put(response.getCursorPosition().getPartition(), response.getCursorPosition());
                    }
                    return new ItemListResponseFlattened<byte[]>(items, cursorPositions);
                });
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
