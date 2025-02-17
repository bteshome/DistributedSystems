package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.responses.ClientMetadataFetchResponse;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Component
@Slf4j
public class ClientMetadataFetcher {
    @Autowired
    ClientSettings clientSettings;
    @Autowired
    WebClient webClient;

    public void fetch() {
        for (String endpoint : clientSettings.getEndpoints().split(",")) {
            try {
                fetch(endpoint);
                return;
            } catch (Exception ignored) {
                log.debug("Unable to fetch metadata from endpoint '{}'.", endpoint);
            }
        }

        log.error("Unable to fetch metadata from any endpoint.");
    }

    private void fetch(String endpoint) {
        ClientMetadataFetchResponse response = webClient
            .post()
            .uri("http://%s/api/metadata/get-metadata/".formatted(endpoint))
            //.accept(MediaType.APPLICATION_JSON)
            .retrieve()
            .toEntity(ClientMetadataFetchResponse.class)
            .block()
            .getBody();

            if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                Map<EntityType, Map<String, Object>> state = JavaSerDe.deserialize(response.getSerializedMetadata());
                MetadataCache.getInstance().setState(state);
                log.debug("Refreshed metadata successfully.");
            } else {
                log.error("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatusCode(), response.getErrorMessage()));
            }
    }
}
