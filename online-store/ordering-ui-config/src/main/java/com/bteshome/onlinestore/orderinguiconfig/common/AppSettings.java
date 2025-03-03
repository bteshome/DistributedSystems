package com.bteshome.onlinestore.orderinguiconfig.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConfigurationProperties
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class AppSettings {
    private Map<String, String> secretLocations;

    public void print() {
        if (secretLocations == null || secretLocations.isEmpty()) {
            log.info("AppSettings: secretLocations is empty.");
            return;
        }

        for (Map.Entry<String, String> entry : secretLocations.entrySet())
            log.info("AppSettings: secretLocations: {}={}", entry.getKey(), entry.getValue());
    }
}
