package com.bteshome.keyvaluestore.client.writers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.clientrequests.BatchWrite;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.*;

@Component
@Slf4j
public class BatchWriter {
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;
    @Autowired
    ItemWriter itemWriter;

    public Flux<ItemPutResponse> putStringBatch(BatchWrite<String> request) {
        List<Map.Entry<String, byte[]>> items = new ArrayList<>();
        List<Map<String, String>> indexes = new ArrayList<>();

        for (int i = 0; i < request.getItems().size(); i++) {
            Map.Entry<String, String> item = request.getItems().get(i);
            String key = Validator.notEmpty(item.getKey(), "Key");
            String value = Validator.notEmpty(item.getValue(), "Value");
            byte[] valueBytes = value.getBytes();
            items.add(new AbstractMap.SimpleEntry<>(key, valueBytes));
            if (request.getIndexKeys() != null)
                indexes.add(request.getIndexKeys().get(i));
        }

        return putBytes(request.getTable(),
                request.getAck(),
                items,
                indexes,
                request.getMaxRetries());
    }

    public <T> Flux<ItemPutResponse> putObjectBatch(BatchWrite<T> request) {
        List<Map.Entry<String, byte[]>> items = new ArrayList<>();
        List<Map<String, String>> indexes = new ArrayList<>();

        for (int i = 0; i < request.getItems().size(); i++) {
            Map.Entry<String, T> item = request.getItems().get(i);
            String key = Validator.notEmpty(item.getKey(), "Key");
            T value = item.getValue();
            byte[] valueBytes = JsonSerDe.serializeToBytes(value);
            items.add(new AbstractMap.SimpleEntry<>(key, valueBytes));
            if (request.getIndexKeys() != null)
                indexes.add(request.getIndexKeys().get(i));
        }

        return putBytes(request.getTable(),
                request.getAck(),
                items,
                indexes,
                request.getMaxRetries());
    }

    public Flux<ItemPutResponse> putBytes(String table,
                                          AckType ack,
                                          List<Map.Entry<String, byte[]>> items,
                                          List<Map<String, String>> indexes,
                                          int maxRetries) {
        table = Validator.notEmpty(table, "Table name");

        HashMap<Integer, ItemPutRequest> partitionRequests = new HashMap<>();

        for (int i = 0; i < items.size(); i++) {
            Map.Entry<String, byte[]> item = items.get(i);
            Map<String, String> itemIndexes = (indexes == null || indexes.isEmpty()) ? null : indexes.get(i);
            int partition = keyToPartitionMapper.map(table, item.getKey());

            if (!partitionRequests.containsKey(partition)) {
                partitionRequests.put(partition, new ItemPutRequest());
                partitionRequests.get(partition).setTable(table);
                partitionRequests.get(partition).setPartition(partition);
                partitionRequests.get(partition).setAck(ack);
            }

            partitionRequests.get(partition).getItems().add(new Item(item.getKey(), item.getValue(), itemIndexes, null));
        }

        int maxBatchSize = (Integer) MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_BATCH_SIZE_MAX_KEY);

        for (HashMap.Entry<Integer, ItemPutRequest> partitionRequest : partitionRequests.entrySet())
            if (partitionRequest.getValue().getItems().size() > maxBatchSize)
                throw new ClientException("Batch size exceeds max batch size of %s for a single partition.".formatted(maxBatchSize));

        return Flux.fromIterable(partitionRequests.entrySet())
                .flatMap(request -> itemWriter.put(request.getValue(), request.getKey(), maxRetries));
    }
}
