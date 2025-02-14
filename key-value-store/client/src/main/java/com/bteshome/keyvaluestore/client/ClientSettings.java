package com.bteshome.keyvaluestore.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
@ConfigurationProperties(prefix = "client")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ClientSettings {
    private String storageNodeEndpoints;
}
