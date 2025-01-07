package com.bteshome.keyvaluestore.storage.common;

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
@ConfigurationProperties(prefix = "storage")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StorageSettings {
    private Map<String, String> node;
    private int heartbeatSendIntervalSeconds;
}
