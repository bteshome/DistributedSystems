package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.ItemDelete;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.requests.ItemDeleteRequest;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemDeleteResponse;
import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.io.Serializable;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ItemWriter extends Writer {
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

        put(itemPutRequest, partition);
    }

    public void delete(ItemDelete request) {
        ItemDeleteRequest itemDeleteRequest = new ItemDeleteRequest();
        itemDeleteRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        itemDeleteRequest.getKeys().add(request.getKey());

        int partition = keyToPartitionMapper.map(request.getTable(), request.getKey());
        itemDeleteRequest.setPartition(partition);

        delete(itemDeleteRequest, partition);
    }
}
