package com.bteshome.keyvaluestore.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "storage-client")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ClientSettings {
    private String endpoints;
}
