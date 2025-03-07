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
import org.apache.logging.log4j.util.Strings;
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
        List<byte[]> values = new ArrayList<>();

        for (int i = 0; i < request.getValues().size(); i++) {
            String value = Validator.notEmpty(request.getValues().get(i), "Value for item " + i);
            byte[] valueBytes = value.getBytes();
            values.add(valueBytes);
        }

        return putBytes(request.getTable(),
                request.getAck(),
                request.getKeys(),
                request.getPartitionKeys(),
                values,
                request.getIndexKeys(),
                request.getMaxRetries());
    }

    public <T> Flux<ItemPutResponse> putObjectBatch(BatchWrite<T> request) {
        List<byte[]> values = new ArrayList<>();

        for (int i = 0; i < request.getValues().size(); i++) {
            T value = Validator.notNull(request.getValues().get(i), "Value for item " + i);
            byte[] valueBytes = JsonSerDe.serializeToBytes(value);
            values.add(valueBytes);
        }

        return putBytes(request.getTable(),
                request.getAck(),
                request.getKeys(),
                request.getPartitionKeys(),
                values,
                request.getIndexKeys(),
                request.getMaxRetries());
    }

    public Flux<ItemPutResponse> putBytesBatch(BatchWrite<byte[]> request) {
        for (int i = 0; i < request.getValues().size(); i++)
            Validator.notNull(request.getValues().get(i), "Value for item " + i);

        return putBytes(request.getTable(),
                request.getAck(),
                request.getKeys(),
                request.getPartitionKeys(),
                request.getValues(),
                request.getIndexKeys(),
                request.getMaxRetries());
    }

    private Flux<ItemPutResponse> putBytes(String table,
                                          AckType ack,
                                          List<String> keys,
                                          List<String> partitionKeys,
                                          List<byte[]> values,
                                          List<Map<String, String>> indexes,
                                          int maxRetries) {
        table = Validator.notEmpty(table, "Table name");
        HashMap<Integer, ItemPutRequest> partitionRequests = new HashMap<>();

        for (int i = 0; i < keys.size(); i++) {
            String key = Validator.notEmpty(keys.get(i), "Key for item " + i);
            String partitionKey = (!partitionKeys.isEmpty() && !Strings.isBlank(partitionKeys.get(i))) ?
                    partitionKeys.get(i) :
                    key;
            byte[] value = values.get(i);
            Map<String, String> itemIndexes = indexes.isEmpty() ? null : indexes.get(i);

            int partition = keyToPartitionMapper.map(table, partitionKey);

            if (!partitionRequests.containsKey(partition)) {
                partitionRequests.put(partition, new ItemPutRequest());
                partitionRequests.get(partition).setTable(table);
                partitionRequests.get(partition).setPartition(partition);
                partitionRequests.get(partition).setAck(ack);
            }

            partitionRequests.get(partition).getItems().add(new Item(key, partitionKey, value, itemIndexes, null));
        }

        int maxBatchSize = (Integer) MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_BATCH_SIZE_MAX_KEY);

        for (HashMap.Entry<Integer, ItemPutRequest> partitionRequest : partitionRequests.entrySet())
            if (partitionRequest.getValue().getItems().size() > maxBatchSize)
                throw new ClientException("Batch size exceeds max batch size of %s for a single partition.".formatted(maxBatchSize));

        return Flux.fromIterable(partitionRequests.entrySet())
                .flatMap(request -> itemWriter.put(request.getValue(), request.getKey(), maxRetries));
    }
}
