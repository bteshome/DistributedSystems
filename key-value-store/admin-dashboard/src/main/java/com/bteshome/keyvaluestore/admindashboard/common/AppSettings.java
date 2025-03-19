package com.bteshome.keyvaluestore.admindashboard.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class AppSettings {
    private String keycloakUrl;
    private String keycloakRealm;
    private String keycloakClientId;
    private boolean securityDisabled;

    public void print() {
        log.info("AppSettings: keycloakRealm={}", keycloakRealm);
        log.info("AppSettings: keycloakRealm={}", keycloakRealm);
        log.info("AppSettings: keycloakClientId={}", keycloakClientId);
        log.info("AppSettings: securityDisabled={}", securityDisabled);
    }
}
