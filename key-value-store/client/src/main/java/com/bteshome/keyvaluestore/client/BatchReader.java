package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.requests.ItemListRequest;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class BatchReader {
    @Value("${client.storage-node-endpoints}")
    private String endpoints;

    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public Map<Integer, List<Map.Entry<String, String>>> listStrings(ItemList request) {
        Map<Integer, List<Map.Entry<String, String>>> result = list(request);

        for (int partition : result.keySet()) {
            for (Map.Entry<String, String> item : result.get(partition)) {
                byte[] bytes = Base64.getDecoder().decode(item.getValue());
                item.setValue(new String(bytes));
            }
        }

        return result;
    }

    public <T> Map<Integer, List<Map.Entry<String, T>>> listObjects(ItemList request) {
        Map<Integer, List<Map.Entry<String, String>>> result = list(request);
        Map<Integer, List<Map.Entry<String, T>>> resultTyped = new HashMap<>();

        for (int partition : result.keySet()) {
            resultTyped.put(partition, new ArrayList<>());
            for (Map.Entry<String, String> item : result.get(partition)) {
                T value = JavaSerDe.deserialize(item.getValue());
                resultTyped.get(partition).add(new AbstractMap.SimpleEntry<>(item.getKey(), value));
            }
        }

        return resultTyped;
    }

    public <T> Map<Integer, List<Map.Entry<String, byte[]>>> listBytes(ItemList request) {
        Map<Integer, List<Map.Entry<String, String>>> result = list(request);
        Map<Integer, List<Map.Entry<String, byte[]>>> resultTyped = new HashMap<>();

        for (int partition : result.keySet()) {
            resultTyped.put(partition, new ArrayList<>());
            for (Map.Entry<String, String> item : result.get(partition)) {
                byte[] value = Base64.getDecoder().decode(item.getValue());
                resultTyped.get(partition).add(new AbstractMap.SimpleEntry<>(item.getKey(), value));
            }
        }

        return resultTyped;
    }

    private Map<Integer, List<Map.Entry<String, String>>> list(ItemList request) {
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
}
