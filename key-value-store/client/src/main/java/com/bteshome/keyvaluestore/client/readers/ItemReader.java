package com.bteshome.keyvaluestore.client.readers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.ClientRetriableException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.clientrequests.ItemGet;
import com.bteshome.keyvaluestore.client.requests.ItemGetRequest;
import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.JavaSerDe;
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
import reactor.util.retry.Retry;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

@Component
@Slf4j
public class ItemReader {
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;
    @Autowired
    WebClient webClient;

    public Mono<String> getString(ItemGet request) {
        return get(request).flatMap(response -> Mono.just(new String(response)));
    }

    public <T extends Serializable> Mono<T> getObject(ItemGet request) {
        return get(request).flatMap(response -> {
            T value = JavaSerDe.deserialize(response);
            return Mono.just(value);
        });
    }

    public Mono<byte[]> getBytes(ItemGet request) {
        return get(request);
    }

    private Mono<byte[]> get(ItemGet request) {
        ItemGetRequest itemGetRequest = new ItemGetRequest();
        itemGetRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        itemGetRequest.setKey(Validator.notEmpty(request.getKey(), "Key"));

        int partition = keyToPartitionMapper.map(request.getTable(), request.getKey());
        itemGetRequest.setPartition(partition);

        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
        if (endpoint == null)
            return Mono.error(new ClientException("Table '%s' partition '%s' is offline.".formatted(request.getTable(), partition)));
        return get(endpoint, itemGetRequest)
                .map(ItemGetResponse::getValue);
    }

    private Mono<ItemGetResponse> get(String endpoint, ItemGetRequest itemGetRequest) {
        return webClient
                .post()
                .uri("http://%s/api/items/get/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(itemGetRequest)
                .retrieve()
                .toEntity(ItemGetResponse.class)
                .map(HttpEntity::getBody)
                .flatMap(response -> {
                    if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
                        return get(response.getLeaderEndpoint(), itemGetRequest);
                    } else if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                        return Mono.just(response);
                    } else {
                        return Mono.error(new ClientException("Unexpected status code: %s, %s".formatted(response.getHttpStatusCode(), response.getErrorMessage())));
                    }
                });
    }
}
