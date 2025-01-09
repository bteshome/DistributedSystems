package com.bteshome.keyvaluestore.storage;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConfigurationProperties(prefix = "storage")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StorageSettings {
    private Map<String, String> node;
}
