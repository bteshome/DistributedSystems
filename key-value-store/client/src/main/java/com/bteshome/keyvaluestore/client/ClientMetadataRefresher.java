package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.client.responses.ClientMetadataFetchResponse;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.MetadataClientSettings;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.Table;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.HashMap;
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
            } catch (Exception e) { }
        }

        log.error("Unable to fetch metadata from any endpoint.");
    }

    private void fetch(String endpoint) {
        ClientMetadataFetchResponse response = RestClient.builder()
                .build()
                .post()
                .uri("http://%s/api/metadata/get-metadata/".formatted(endpoint))
                .retrieve()
                .toEntity(ClientMetadataFetchResponse.class)
                .getBody();

        if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
            Map<EntityType, Map<String, Object>> state = JavaSerDe.deserialize(response.getSerializedMetadata());
            MetadataCache.getInstance().setState(state);
            log.debug("Refreshed metadata successfully.");
            return;
        }

        throw new RuntimeException("Unable to read from endpoint '%s'. Http status: %s, error: %s.".formatted(endpoint, response.getHttpStatusCode(), response.getErrorMessage()));
    }
}
