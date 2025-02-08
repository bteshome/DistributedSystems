package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.responses.ClientMetadataFetchResponse;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.MetadataClientSettings;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ClientMetadataRefresher implements CommandLineRunner {
    private ScheduledExecutorService executor = null;
    @Value("${client.storage-node-endpoints}")
    private String endpoints;
    @Autowired
    MetadataClientSettings metadataClientSettings;
    @Autowired
    WebClient webClient;

    @Override
    public void run(String... args) throws Exception {
        schedule();
    }

    public void schedule() {
        try {
            executor = Executors.newSingleThreadScheduledExecutor();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    executor.close();
                }
            });
            executor.scheduleAtFixedRate(this::fetch,
                    0L,
                    metadataClientSettings.getMetadataRefreshIntervalMs(),
                    TimeUnit.MILLISECONDS);
            log.info("Scheduled metadata refresher. The interval is {} ms.", metadataClientSettings.getMetadataRefreshIntervalMs());
        } catch (Exception e) {
            log.error("Error scheduling metadata refresher: ", e);
        }
    }

    public void fetch() {
        for (String endpoint : endpoints.split(",")) {
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
