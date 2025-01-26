package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.BatchWrite;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.entities.Item;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class ItemWriter {
    @Value("${client.storage-node-endpoints}")
    private String endpoints;

    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public void put(ItemWrite request) {
        ItemPutRequest itemPutRequest = new ItemPutRequest();
        itemPutRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        Requests.validateItem(request.getItem());
        itemPutRequest.getItems().add(request.getItem());

        int partition = keyToPartitionMapper.map(request.getTable(), request.getItem().getKey());
        itemPutRequest.setPartition(partition);

        put(itemPutRequest);
    }

    public void putBatch(BatchWrite request) {
        request.setTable(Validator.notEmpty(request.getTable(), "Table name"));

        HashMap<Integer, ItemPutRequest> partitionRequests = new HashMap<Integer, ItemPutRequest>();

        for (Item item : request.getItems()) {
            Requests.validateItem(item);

            int partition = keyToPartitionMapper.map(request.getTable(), item.getKey());

            if (!partitionRequests.containsKey(partition)) {
                partitionRequests.put(partition, new ItemPutRequest());
                partitionRequests.get(partition).setTable(request.getTable());
                partitionRequests.get(partition).setPartition(partition);
            }

            partitionRequests.get(partition).getItems().add(item);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (HashMap.Entry<Integer, ItemPutRequest> partitionRequest : partitionRequests.entrySet())
            futures.add(CompletableFuture.runAsync(() -> put(partitionRequest.getValue()) ));

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void put(ItemPutRequest itemPutRequest) {
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
