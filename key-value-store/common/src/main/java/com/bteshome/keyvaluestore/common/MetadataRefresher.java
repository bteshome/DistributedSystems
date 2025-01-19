package com.bteshome.keyvaluestore.common;

import com.bteshome.keyvaluestore.common.requests.MetadataRefreshRequest;
import com.bteshome.keyvaluestore.common.responses.MetadataRefreshResponse;
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
public class MetadataRefresher {
    private ScheduledExecutorService executor = null;

    @Autowired
    ClientSettings clientSettings;

    @Autowired
    ClientBuilder clientBuilder;

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
                    clientSettings.getMetadataRefreshIntervalMs(),
                    TimeUnit.MILLISECONDS);
            log.info("Scheduled metadata refresher. The interval is {} ms.", clientSettings.getMetadataRefreshIntervalMs());
        } catch (Exception e) {
            log.error("Error scheduling metadata refresher: ", e);
        }
    }

    public void fetch() {
        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            MetadataRefreshRequest request = new MetadataRefreshRequest(
                    clientSettings.getClientId(),
                    MetadataCache.getInstance().getLastFetchedVersion());
            final RaftClientReply reply = client.io().sendReadOnly(request);

            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);

                if (ResponseStatus.extractStatusCode(messageString) == HttpStatus.OK.value()) {
                    MetadataRefreshResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
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
