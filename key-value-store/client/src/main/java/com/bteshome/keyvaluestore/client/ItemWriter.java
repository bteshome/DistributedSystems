package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.MetadataRefresher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
@Slf4j
public class ItemWriter {
    @Value("${client.storage-node-endpoints}")
    private String endpoints;

    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;

    public void put(ItemPutRequest request) {
        request.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        request.setKey(Validator.notEmpty(request.getKey(), "Key"));
        request.setValue(Validator.notEmpty(request.getValue(), "Value"));
        Validator.doesNotContain(request.getValue(), ",", "Value");

        int partition = keyToPartitionMapper.map(request.getTable(), request.getKey());
        request.setPartition(partition);

        for (String endpoint : endpoints.split(",")) {
            try {
                put(endpoint, request);
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

        if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
            return;
        }

        throw new RuntimeException("Unable to write to endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatusCode(), response.getErrorMessage()));
    }
}
