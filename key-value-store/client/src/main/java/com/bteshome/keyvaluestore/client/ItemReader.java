package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.ItemGet;
import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.requests.ItemCountAndOffsetsRequest;
import com.bteshome.keyvaluestore.client.requests.ItemGetRequest;
import com.bteshome.keyvaluestore.client.requests.ItemListRequest;
import com.bteshome.keyvaluestore.client.responses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class ItemReader {
    @Value("${client.storage-node-endpoints}")
    private String endpoints;

    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public String get(ItemGet request) {
        ItemGetRequest itemGetRequest = new ItemGetRequest();
        itemGetRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        itemGetRequest.setKey(Validator.notEmpty(request.getKey(), "Key"));

        int partition = keyToPartitionMapper.map(request.getTable(), request.getKey());
        itemGetRequest.setPartition(partition);

        for (String endpoint : endpoints.split(",")) {
            try {
                return get(endpoint, itemGetRequest);
            } catch (Exception e) {
                log.trace(e.getMessage());
            }
        }

        throw new RuntimeException("Unable to read from any endpoint.");
    }

    private String get(String endpoint, ItemGetRequest request) {
        ItemGetResponse response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/get/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemGetResponse.class)
                .getBody();

        if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value())
            return get(response.getLeaderEndpoint(), request);

        if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value())
            return null;

        if (response.getHttpStatusCode() == HttpStatus.OK.value())
            return response.getValue();

        throw new RuntimeException("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatusCode(), response.getErrorMessage()));
    }

    public Map<Integer, List<Map.Entry<String, String>>> list(ItemList request) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(request.getTable());
        final Map<Integer, List<Map.Entry<String, String>>> result = new ConcurrentHashMap<>();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int partition = 1; partition <= numPartitions; partition++) {
            final int partitionFinal = partition;
            futures.add(CompletableFuture.runAsync(() -> {
                ItemListRequest itemListRequest = new ItemListRequest();
                itemListRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
                itemListRequest.setPartition(partitionFinal);
                itemListRequest.setLimit(request.getLimit());

                for (String endpoint : endpoints.split(",")) {
                    try {
                        result.put(partitionFinal, list(endpoint, itemListRequest));
                        return;
                    } catch (Exception e) {
                        log.trace(e.getMessage());
                    }
                }

                throw new RuntimeException("Unable to read from any endpoint.");
            }));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private List<Map.Entry<String, String>> list(String endpoint, ItemListRequest request) {
        ItemListResponse response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/list/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemListResponse.class)
                .getBody();

        if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value())
            return list(response.getLeaderEndpoint(), request);

        if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value())
            return List.of();

        if (response.getHttpStatusCode() == HttpStatus.OK.value())
            return response.getItems();

        throw new RuntimeException("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatusCode(), response.getErrorMessage()));
    }

    public ItemCountAndOffsetsResponse getCountAndOffsets(ItemCountAndOffsetsRequest request) {
        request.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        for (String endpoint : endpoints.split(",")) {
            try {
                return getCountAndOffsets(endpoint, request);
            } catch (Exception e) {
                log.trace(e.getMessage());
            }
        }
        return null;
    }

    private ItemCountAndOffsetsResponse getCountAndOffsets(String endpoint, ItemCountAndOffsetsRequest request) {
        ItemCountAndOffsetsResponse response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/count-and-offsets/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemCountAndOffsetsResponse.class)
                .getBody();

        if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value())
            return getCountAndOffsets(response.getLeaderEndpoint(), request);

        if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value())
            return null;

        if (response.getHttpStatusCode() == HttpStatus.OK.value())
            return response;

        throw new RuntimeException("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatusCode(), response.getErrorMessage()));
    }
}
