package com.bteshome.keyvaluestore.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class ItemReader {
    @Value("${client.storage-node-endpoints}")
    private String endpoints;

    public String get(ItemGetRequest request) {
        request.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        request.setKey(Validator.notEmpty(request.getKey(), "Key"));
        for (String endpoint : endpoints.split(",")) {
            try {
                return get(endpoint, request);
            } catch (Exception e) {
                log.trace(e.getMessage());
            }
        }
        throw new RuntimeException("Unable to read from any endpoint.");
    }

    private String get(String endpoint, ItemGetRequest request) {
        ItemGetResponse response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/get/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemGetResponse.class)
                .getBody();

        if (response.getHttpStatus() == HttpStatus.MOVED_PERMANENTLY.value()) {
            return get(response.getLeaderEndpoint(), request);
        }

        if (response.getHttpStatus() == HttpStatus.NOT_FOUND.value()) {
            return null;
        }

        if (response.getHttpStatus() == HttpStatus.OK.value()) {
            return response.getValue();
        }

        throw new RuntimeException("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatus(), response.getErrorMessage()));
    }

    public List<Map.Entry<String, String>> list(ItemListRequest request) {
        request.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        request.setPartition(Validator.positive(request.getPartition(), "Partition"));
        request.setLimit(Validator.setDefault(request.getLimit(), 10));
        for (String endpoint : endpoints.split(",")) {
            try {
                return list(endpoint, request);
            } catch (Exception e) {
                log.trace(e.getMessage());
            }
        }
        throw new RuntimeException("Unable to read from any endpoint.");
    }

    private List<Map.Entry<String, String>> list(String endpoint, ItemListRequest request) {
        ItemListResponse response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/list/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemListResponse.class)
                .getBody();

        if (response.getHttpStatus() == HttpStatus.MOVED_PERMANENTLY.value()) {
            return list(response.getLeaderEndpoint(), request);
        }

        if (response.getHttpStatus() == HttpStatus.NOT_FOUND.value()) {
            return List.of();
        }

        if (response.getHttpStatus() == HttpStatus.OK.value()) {
            return response.getItems();
        }

        throw new RuntimeException("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatus(), response.getErrorMessage()));
    }

    public int count(ItemCountRequest request) {
        request.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        for (String endpoint : endpoints.split(",")) {
            try {
                return count(endpoint, request);
            } catch (Exception e) {
                log.trace(e.getMessage());
            }
        }
        throw new RuntimeException("Unable to read from any endpoint.");
    }

    private int count(String endpoint, ItemCountRequest request) {
        ItemCountResponse response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/items/count/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toEntity(ItemCountResponse.class)
                .getBody();

        if (response.getHttpStatus() == HttpStatus.MOVED_PERMANENTLY.value()) {
            return count(response.getLeaderEndpoint(), request);
        }

        if (response.getHttpStatus() == HttpStatus.NOT_FOUND.value()) {
            return 0;
        }

        if (response.getHttpStatus() == HttpStatus.OK.value()) {
            return response.getCount();
        }

        throw new RuntimeException("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatus(), response.getErrorMessage()));
    }
}
