package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.clientrequests.ItemGet;
import com.bteshome.keyvaluestore.client.requests.ItemGetRequest;
import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
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

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ItemReader {
    @Value("${client.storage-node-endpoints}")
    private String endpoints;

    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public String getString(ItemGet request) {
        ItemGetResponse response = get(request);
        if (response == null)
            return null;
        byte[] bytes = Base64.getDecoder().decode(response.getValue());
        return new String(bytes);
    }

    public <T extends Serializable> T getObject(ItemGet request) {
        ItemGetResponse response = get(request);
        if (response == null)
            return null;
        T value = JavaSerDe.deserialize(response.getValue());
        return value;
    }

    public byte[] getBytes(ItemGet request) {
        ItemGetResponse response = get(request);
        if (response == null)
            return null;
        return Base64.getDecoder().decode(response.getValue());
    }

    private ItemGetResponse get(ItemGet request) {
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
            return response;

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
}
