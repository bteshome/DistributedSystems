package com.bteshome.keyvaluestore.metadata.common;

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
@ConfigurationProperties(prefix = "metadata-server")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Settings {
    private UUID groupId;
    private String storageDir;
    private Map<String, String> node;
    private List<Map<String, String>> peers;
}
