package com.bteshome.keyvaluestore.client.adminreaders;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.clientrequests.ItemGet;
import com.bteshome.keyvaluestore.client.adminrequests.ItemVersionsGetRequest;
import com.bteshome.keyvaluestore.client.adminresponses.ItemVersionsGetResponse;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class ItemVersionsReader {
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;
    @Autowired
    WebClient webClient;

    public Mono<List<String>> getString(ItemGet request) {
        return get(request).map(valueVersions -> valueVersions
            .stream()
            .map(bytes -> bytes == null ? "null" : new String(bytes))
            .toList());
    }

    private Mono<List<byte[]>> get(ItemGet request) {
        ItemVersionsGetRequest itemVersionsGetRequest = new ItemVersionsGetRequest();
        itemVersionsGetRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        itemVersionsGetRequest.setKey(Validator.notEmpty(request.getKey(), "Key"));

        int partition = keyToPartitionMapper.map(request.getTable(), request.getKey());
        itemVersionsGetRequest.setPartition(partition);

        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
        if (endpoint == null)
            return Mono.error(new ClientException("Table '%s' partition '%s' is offline.".formatted(request.getTable(), partition)));
        return get(endpoint, itemVersionsGetRequest).map(ItemVersionsGetResponse::getValueVersions);
    }

    private Mono<ItemVersionsGetResponse> get(String endpoint, ItemVersionsGetRequest request) {
        return webClient
                .post()
                .uri("http://%s/api/items/get-versions/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .toEntity(ItemVersionsGetResponse.class)
                .map(HttpEntity::getBody)
                .flatMap(response -> {
                    if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
                        return get(response.getLeaderEndpoint(), request);
                    } else if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                        return Mono.just(response);
                    } else {
                        return Mono.error(new ClientException("Unexpected status code: %s, %s".formatted(response.getHttpStatusCode(), response.getErrorMessage())));
                    }
                });
    }
}
