package com.bteshome.keyvaluestore.client.writers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.ClientRetriableException;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.common.entities.Item;
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
import java.util.Base64;

@Component
@Slf4j
public class ItemWriter {
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;
    @Autowired
    WebClient webClient;

    public Mono<ItemPutResponse> putString(ItemWrite<String> request) {
        byte[] bytes = request.getValue().getBytes();
        return putBytes(request.getTable(), request.getKey(), request.getAck(), bytes, request.getMaxRetries());
    }

    public <T extends Serializable> Mono<ItemPutResponse> putObject(ItemWrite<T> request) {
        byte[] bytes = JavaSerDe.serializeToBytes(request.getValue());
        return putBytes(request.getTable(), request.getKey(), request.getAck(), bytes, request.getMaxRetries());
    }

    public Mono<ItemPutResponse> putBytes(String table, String key, AckType ack, byte[] bytes, int maxRetries) {
        String base64EncodedString = Base64.getEncoder().encodeToString(bytes);

        ItemPutRequest itemPutRequest = new ItemPutRequest();
        itemPutRequest.setTable(Validator.notEmpty(table, "Table name"));
        itemPutRequest.getItems().add(new Item(key, base64EncodedString));
        itemPutRequest.setAck(ack);

        int partition = keyToPartitionMapper.map(table, key);
        itemPutRequest.setPartition(partition);

        return put(itemPutRequest, partition, maxRetries);
    }

    Mono<ItemPutResponse> put(ItemPutRequest itemPutRequest, int partition, int maxRetries) {
        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(itemPutRequest.getTable(), partition);
        if (endpoint == null)
            return Mono.error(new ClientException("Table '%s' partition '%s' is offline.".formatted(itemPutRequest.getTable(), partition)));
        return put(endpoint, itemPutRequest, maxRetries);
    }

    private Mono<ItemPutResponse> put(String endpoint, ItemPutRequest itemPutRequest, int maxRetries) {
        return webClient
                .post()
                .uri("http://%s/api/items/put/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(itemPutRequest)
                .retrieve()
                .toEntity(ItemPutResponse.class)
                .map(HttpEntity::getBody)
                .flatMap(response -> {
                    if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
                        return put(response.getLeaderEndpoint(), itemPutRequest, maxRetries);
                    } else if (response.getHttpStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR.value() ||
                               response.getHttpStatusCode() == HttpStatus.REQUEST_TIMEOUT.value()) {
                        return Mono.error(new ClientRetriableException("Unexpected status code: %s, %s".formatted(response.getHttpStatusCode(), response.getErrorMessage())));
                    } else {
                        return Mono.just(response);
                    }
                })
                .retryWhen(Retry.fixedDelay(maxRetries, Duration.ofSeconds(2)).filter(ex -> ex instanceof ClientRetriableException));
    }
}
