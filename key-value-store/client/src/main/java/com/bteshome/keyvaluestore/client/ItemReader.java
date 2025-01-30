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
import java.util.concurrent.TimeUnit;

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

        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
        int retries = 0;

        ItemGetResponse response = get(endpoint, itemGetRequest);

        while (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value() && retries < 3) {
            retries++;
            try {
                TimeUnit.SECONDS.sleep(1L);
            } catch (Exception ignored) { }
            response = get(endpoint, itemGetRequest);
        }

        if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value())
            return null;

        if (response.getHttpStatusCode() == HttpStatus.OK.value())
            return response.getValue();

        throw new RuntimeException("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatusCode(), response.getErrorMessage()));
    }

    private ItemGetResponse get(String endpoint, ItemGetRequest request) {
        return RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/get/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemGetResponse.class)
                .getBody();
    }

    public Map<Integer, List<Map.Entry<String, String>>> list(ItemList request) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(request.getTable());
        final Map<Integer, List<Map.Entry<String, String>>> result = new ConcurrentHashMap<>();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int partition = 1; partition <= numPartitions; partition++) {
            final int partitionFinal = partition;
            final String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
            futures.add(CompletableFuture.runAsync(() -> {
                ItemListRequest itemListRequest = new ItemListRequest();
                itemListRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
                itemListRequest.setPartition(partitionFinal);
                itemListRequest.setLimit(request.getLimit());
                int retries = 0;

                try {
                    ItemListResponse response = list(endpoint, itemListRequest);

                    while (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value() && retries < 3) {
                        retries++;
                        try {
                            TimeUnit.SECONDS.sleep(1L);
                        } catch (Exception ignored) {
                        }
                        response = list(endpoint, itemListRequest);
                    }

                    if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value())
                        return;

                    if (response.getHttpStatusCode() == HttpStatus.OK.value())
                        result.put(partitionFinal, response.getItems());
                } catch (Exception e) {
                    log.debug(e.getMessage(), e);
                }
            }));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private ItemListResponse  list(String endpoint, ItemListRequest request) {
        return RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/list/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemListResponse.class)
                .getBody();
    }

    public ItemCountAndOffsetsResponse getCountAndOffsets(ItemCountAndOffsetsRequest request) {
        request.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        final String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), request.getPartition());
        int retries = 0;

        try {
            ItemCountAndOffsetsResponse response = getCountAndOffsets(endpoint, request);

            while (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value() && retries < 3) {
                retries++;
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (Exception ignored) { }
                response = getCountAndOffsets(endpoint, request);
            }

            if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value())
                return null;

            if (response.getHttpStatusCode() == HttpStatus.OK.value())
                return response;
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
        }

        return null;
    }

    private ItemCountAndOffsetsResponse getCountAndOffsets(String endpoint, ItemCountAndOffsetsRequest request) {
        return RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/count-and-offsets/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemCountAndOffsetsResponse.class)
                .getBody();

     }
}
