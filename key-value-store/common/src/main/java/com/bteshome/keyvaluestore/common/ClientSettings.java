package com.bteshome.keyvaluestore.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@ConfigurationProperties(prefix = "client")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ClientSettings {
    private UUID clientId;
    private UUID groupId;
    private List<PeerInfo> peers;
    private long metadataRefreshIntervalMs = 60000;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PeerInfo {
        private String id;
        private String host;
        private int port;
    }
}
