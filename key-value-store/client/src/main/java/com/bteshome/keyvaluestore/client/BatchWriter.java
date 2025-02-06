package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.BatchDelete;
import com.bteshome.keyvaluestore.client.clientrequests.BatchWrite;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.ItemDeleteRequest;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class BatchWriter extends Writer {
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public void putStringBatch(BatchWrite<String> request) {
        List<Map.Entry<String, byte[]>> items = new ArrayList<>();

        for (Map.Entry<String, String> item : request.getItems()) {
            String key = Validator.notEmpty(item.getKey(), "Key");
            String value = Validator.notEmpty(item.getValue(), "Value");
            byte[] bytes = value.getBytes();
            items.add(new AbstractMap.SimpleEntry<>(key, bytes));
        }

        putBytes(request.getTable(), request.getAck(), items);
    }

    public <T> void putObjectBatch(BatchWrite<T> request) {
        List<Map.Entry<String, byte[]>> items = new ArrayList<>();

        for (Map.Entry<String, T> item : request.getItems()) {
            String key = Validator.notEmpty(item.getKey(), "Key");
            T value = item.getValue();
            byte[] bytes = JavaSerDe.serializeToBytes(value);
            items.add(new AbstractMap.SimpleEntry<>(key, bytes));
        }

        putBytes(request.getTable(), request.getAck(), items);
    }

    public void putBytes(String table, AckType ack, List<Map.Entry<String, byte[]>> items) {
        table = Validator.notEmpty(table, "Table name");

        HashMap<Integer, ItemPutRequest> partitionRequests = new HashMap<>();

        for (Map.Entry<String, byte[]> item : items) {
            String base64EncodedString = Base64.getEncoder().encodeToString(item.getValue());

            int partition = keyToPartitionMapper.map(table, item.getKey());

            if (!partitionRequests.containsKey(partition)) {
                partitionRequests.put(partition, new ItemPutRequest());
                partitionRequests.get(partition).setTable(table);
                partitionRequests.get(partition).setPartition(partition);
                partitionRequests.get(partition).setAck(ack);
            }

            partitionRequests.get(partition).getItems().add(new Item(item.getKey(), base64EncodedString));
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int maxBatchSize = (Integer) MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_BATCH_SIZE_MAX_KEY);

        for (HashMap.Entry<Integer, ItemPutRequest> partitionRequest : partitionRequests.entrySet()) {
            if (partitionRequest.getValue().getItems().size() > maxBatchSize)
                throw new RuntimeException("Batch size exceeds max batch size of %s for a single partition.".formatted(maxBatchSize));
            futures.add(CompletableFuture.runAsync(() -> put(partitionRequest.getValue(), partitionRequest.getKey())));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteBatch(BatchDelete request) {
        for (String key: request.getKeys())
            Validator.notEmpty(key, "Key");
        String table = Validator.notEmpty(request.getTable(), "Table name");

        HashMap<Integer, ItemDeleteRequest> partitionRequests = new HashMap<>();

        for (String key: request.getKeys()) {
            int partition = keyToPartitionMapper.map(table, key);

            if (!partitionRequests.containsKey(partition)) {
                partitionRequests.put(partition, new ItemDeleteRequest());
                partitionRequests.get(partition).setTable(table);
                partitionRequests.get(partition).setPartition(partition);
                partitionRequests.get(partition).setAck(request.getAck());
            }

            partitionRequests.get(partition).getKeys().add(key);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // TODO - should the maximum batch size for a delete operation be different from that for a put operation?
        int maxBatchSize = (Integer) MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_BATCH_SIZE_MAX_KEY);

        for (HashMap.Entry<Integer, ItemDeleteRequest> partitionRequest : partitionRequests.entrySet()) {
            if (partitionRequest.getValue().getKeys().size() > maxBatchSize)
                throw new RuntimeException("Batch size exceeds max batch size of %s for a single partition.".formatted(maxBatchSize));
            futures.add(CompletableFuture.runAsync(() -> delete(partitionRequest.getValue(), partitionRequest.getKey())));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
