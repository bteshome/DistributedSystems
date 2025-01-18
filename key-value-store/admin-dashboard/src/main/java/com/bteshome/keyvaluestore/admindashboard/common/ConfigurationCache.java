package com.bteshome.keyvaluestore.admindashboard.common;

import com.bteshome.keyvaluestore.common.ClientBuilder;
import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.requests.ConfigurationListRequest;
import com.bteshome.keyvaluestore.common.responses.ConfigurationListResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
@Slf4j
public class ConfigurationCache {
    @Getter
    private Map<String, Object> configurations = null;

    @Autowired
    ClientBuilder clientBuilder;

    public void fetch() {
        try (RaftClient client = this.clientBuilder.createRaftClient()) {
            ConfigurationListRequest request = new ConfigurationListRequest();
            final RaftClientReply reply = client.io().sendReadOnly(request);

            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                ConfigurationListResponse response = JavaSerDe.deserialize(messageString.split(" ")[1]);
                configurations = response.getConfigurations();
                log.debug("Cluster configuration fetched.");
                return;
            }

            log.error("Error refreshing cluster configuration.", reply.getException());
        } catch (Exception e) {
            log.error("Error refreshing cluster configuration.", e);
        }
    }
}
