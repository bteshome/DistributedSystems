package com.bteshome.keyvaluestore.client.readers;

import com.bteshome.keyvaluestore.client.requests.ItemCountAndOffsetsRequest;
import com.bteshome.keyvaluestore.client.responses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class CountAndOffsetReader {
    @Autowired
    WebClient webClient;

    public ItemCountAndOffsetsResponse getCountAndOffsets(ItemCountAndOffsetsRequest request) {
        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(request.getTable(), request.getPartition());
        return getCountAndOffsets(endpoint, request);
    }

    public ItemCountAndOffsetsResponse getCountAndOffsets(ItemCountAndOffsetsRequest request, String endpoint) {
        request.setTable(Validator.notEmpty(request.getTable(), "Table name"));
        int retries = 0;

        try {
            ItemCountAndOffsetsResponse response = getCountAndOffsets(endpoint, request);

            while (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value() && retries < 3) {
                retries++;
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (Exception ignored) { }
                response = getCountAndOffsets(endpoint, request);
            }

            if (response.getHttpStatusCode() == HttpStatus.NOT_FOUND.value())
                return null;

            if (response.getHttpStatusCode() == HttpStatus.OK.value())
                return response;
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
        }

        return null;
    }

    private ItemCountAndOffsetsResponse getCountAndOffsets(String endpoint, ItemCountAndOffsetsRequest request) {
        return webClient
                .post()
                .uri("http://%s/api/items/count-and-offsets/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .toEntity(ItemCountAndOffsetsResponse.class)
                .block()
                .getBody();
     }
}
