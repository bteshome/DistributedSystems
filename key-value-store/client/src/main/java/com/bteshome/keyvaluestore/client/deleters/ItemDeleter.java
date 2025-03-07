package com.bteshome.keyvaluestore.client.deleters;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.ClientRetriableException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.clientrequests.ItemDelete;
import com.bteshome.keyvaluestore.client.requests.ItemDeleteRequest;
import com.bteshome.keyvaluestore.client.responses.ItemDeleteResponse;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Component
@Slf4j
public class ItemDeleter {
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;
    @Autowired
    WebClient webClient;

    public Mono<ItemDeleteResponse> delete(ItemDelete request) {
        ItemDeleteRequest itemDeleteRequest = new ItemDeleteRequest();
        itemDeleteRequest.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        itemDeleteRequest.getKeys().add(request.getKey());
        itemDeleteRequest.setAck(request.getAck());

        String partitionKey = request.getPartitionKey();
        if (Strings.isBlank(partitionKey))
            partitionKey = request.getKey();

        int partition = keyToPartitionMapper.map(request.getTable(), partitionKey);
        itemDeleteRequest.setPartition(partition);

        return delete(itemDeleteRequest, partition, request.getRetries());
    }

    Mono<ItemDeleteResponse> delete(ItemDeleteRequest itemDeleteRequest, int partition, int maxRetries) {
        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(itemDeleteRequest.getTable(), partition);
        if (endpoint == null)
            return Mono.error(new ClientException("Table '%s' partition '%s' is offline.".formatted(itemDeleteRequest.getTable(), partition)));
        return delete(endpoint, itemDeleteRequest, maxRetries);
    }

    private Mono<ItemDeleteResponse> delete(String endpoint, ItemDeleteRequest request, int maxRetries) {
        return webClient
                .post()
                .uri("http://%s/api/items/delete/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .toEntity(ItemDeleteResponse.class)
                .map(HttpEntity::getBody)
                .flatMap(response -> {
                    if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
                        return delete(response.getLeaderEndpoint(), request, maxRetries);
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
