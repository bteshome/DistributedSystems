package com.bteshome.keyvaluestore.client.readers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.ClientRetriableException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.clientrequests.ItemGet;
import com.bteshome.keyvaluestore.client.requests.ItemGetRequest;
import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.*;
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
        return getBytes(request).flatMap(response -> {
            String value = new String(response.getValue());
            return Mono.just(value);
        });
    }

    public <T extends Serializable> Mono<T> getObject(ItemGet request, Class<T> clazz) {
        return getBytes(request).flatMap(response -> {
            T value = JavaSerDe.deserialize(response.getValue());
            return Mono.just(value);
        });
    }

    public <T extends Serializable> Mono<Tuple<T, LogPosition>> getVersionedObject(ItemGet request, Class<T> clazz) {
        return getBytes(request).flatMap(response -> {
            T value = JavaSerDe.deserialize(response.getValue());
            LogPosition version = response.getVersion();
            return Mono.just(Tuple.of(value, version));
        });
    }

    public Mono<Tuple<byte[], LogPosition>> getBytesAndVersion(ItemGet request) {
        return getBytes(request).flatMap(response -> {
            return Mono.just(Tuple.of(response.getValue(), response.getVersion()));
        });
    }

    private Mono<ItemGetResponse> getBytes(ItemGet request) {
        ItemGetRequest itemGetRequest = new ItemGetRequest();
        itemGetRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        itemGetRequest.setKey(Validator.notEmpty(request.getKey(), "Key"));
        itemGetRequest.setIsolationLevel(request.getIsolationLevel());

        int partition = keyToPartitionMapper.map(request.getTable(), request.getKey());
        itemGetRequest.setPartition(partition);

        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), partition);
        if (endpoint == null)
            return Mono.error(new ClientException("Table '%s' partition '%s' is offline.".formatted(request.getTable(), partition)));
        return get(endpoint, itemGetRequest);
    }

    private Mono<ItemGetResponse> get(String endpoint, ItemGetRequest itemGetRequest) {
        return webClient
                .post()
                .uri("http://%s/api/items/get/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(itemGetRequest)
                .retrieve()
                .toEntity(ItemGetResponse.class)
                .map(HttpEntity::getBody)
                .flatMap(response -> {
                    if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
                        return get(response.getLeaderEndpoint(), itemGetRequest);
                    } else if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                        return Mono.just(response);
                    } else if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value()) {
                        return Mono.empty();
                    } else {
                        return Mono.error(new ClientException("Unexpected status code: %s, %s".formatted(response.getHttpStatusCode(), response.getErrorMessage())));
                    }
                });
    }
}
