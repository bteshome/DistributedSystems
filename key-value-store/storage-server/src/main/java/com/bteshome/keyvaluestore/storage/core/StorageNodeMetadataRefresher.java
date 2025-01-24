package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.requests.StorageNodeMetadataRefreshRequest;
import com.bteshome.keyvaluestore.common.responses.StorageNodeMetadataRefreshResponse;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class StorageNodeMetadataRefresher {
    private ScheduledExecutorService executor = null;

    @Autowired
    StorageSettings storageSettings;

    @Autowired
    MetadataClientBuilder metadataClientBuilder;

    public void schedule() {
        try {
            fetch();
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.STORAGE_NODE_METADATA_REFRESH_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    executor.close();
                }
            });
            executor.scheduleAtFixedRate(this::fetch,
                    0L,
                    interval,
                    TimeUnit.MILLISECONDS);
            log.info("Scheduled metadata refresher. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling metadata refresher: ", e);
        }
    }

    public void fetch() {
        try (RaftClient client = this.metadataClientBuilder.createRaftClient()) {
            StorageNodeMetadataRefreshRequest request = new StorageNodeMetadataRefreshRequest(
                    storageSettings.getNode().getId(),
                    MetadataCache.getInstance().getLastFetchedVersion());
            final RaftClientReply reply = client.io().sendReadOnly(request);

            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);

                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    StorageNodeMetadataRefreshResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                    MetadataCache.getInstance().setHeartbeatEndpoint(response.getHeartbeatEndpoint());
                    if (response.isModified()) {
                        MetadataCache.getInstance().setState(response.getState());
                        log.debug("Refreshed metadata successfully.");
                    } else {
                        log.debug("Metadata is up to date. Nothing to refresh.");
                    }
                    return;
                }

                log.error("Error refreshing metadata.");
                return;
            }

            log.error("Error refreshing metadata.", reply.getException());
        } catch (Exception e) {
            log.error("Error refreshing metadata.", e);
        }
    }
}
