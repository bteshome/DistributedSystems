package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Base64;

@Component
@Slf4j
public class ItemWriter extends Writer {
    @Value("${client.storage-node-endpoints}")
    private String endpoints;

    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public void putString(ItemWrite<String> request) {
        byte[] bytes = request.getValue().getBytes();
        putBytes(request.getTable(), request.getKey(), bytes);
    }

    public <T extends Serializable> void putObject(ItemWrite<T> request) {
        byte[] bytes = JavaSerDe.serializeToBytes(request.getValue());
        putBytes(request.getTable(), request.getKey(), bytes);
    }

    public void putBytes(String table, String key, byte[] bytes) {
        String base64EncodedString = Base64.getEncoder().encodeToString(bytes);

        ItemPutRequest itemPutRequest = new ItemPutRequest();
        itemPutRequest.setTable(Validator.notEmpty(table, "Table name"));
        itemPutRequest.getItems().add(new Item(key, base64EncodedString));

        int partition = keyToPartitionMapper.map(table, key);
        itemPutRequest.setPartition(partition);

        setEndpoints(endpoints);
        put(itemPutRequest);
    }
}
