package com.bteshome.keyvaluestore.client.deleters;

import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.clientrequests.BatchDelete;
import com.bteshome.keyvaluestore.client.requests.ItemDeleteRequest;
import com.bteshome.keyvaluestore.client.responses.ItemDeleteResponse;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.*;

@Component
@Slf4j
public class BatchDeleter {
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;
    @Autowired
    ItemDeleter itemDeleter;

    public Flux<ItemDeleteResponse> deleteBatch(BatchDelete request) {
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

        // TODO - should the maximum batch size for a delete operation be different from that for a put operation?
        int maxBatchSize = (Integer) MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_BATCH_SIZE_MAX_KEY);

        for (HashMap.Entry<Integer, ItemDeleteRequest> partitionRequest : partitionRequests.entrySet())
            if (partitionRequest.getValue().getKeys().size() > maxBatchSize)
                throw new RuntimeException("Batch size exceeds max batch size of %s for a single partition.".formatted(maxBatchSize));

        return Flux.fromIterable(partitionRequests.entrySet())
                .flatMap(partitionRequest -> itemDeleter.delete(
                        partitionRequest.getValue(),
                        partitionRequest.getKey(),
                        request.getMaxRetries()));
    }
}
