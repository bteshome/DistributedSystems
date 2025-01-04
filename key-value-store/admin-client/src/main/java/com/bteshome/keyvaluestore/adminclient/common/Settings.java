package com.bteshome.keyvaluestore.adminclient.common;

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
@ConfigurationProperties(prefix = "admin-client")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Settings {
    private UUID clientId;
}
