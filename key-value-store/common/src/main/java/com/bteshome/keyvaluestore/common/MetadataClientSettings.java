package com.bteshome.keyvaluestore.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
@ConfigurationProperties(prefix = "metadata-client")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class MetadataClientSettings {
    private UUID clientId;
    private UUID groupId;
    private List<PeerInfo> peers;

    public void print() {
        log.info("MetadataClientSettings: clientId={}", clientId);
        log.info("MetadataClientSettings: groupId={}", groupId);
        log.info("MetadataClientSettings: peers={}", peers);
    }
}
