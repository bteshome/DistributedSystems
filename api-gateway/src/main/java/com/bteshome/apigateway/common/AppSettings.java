package com.bteshome.apigateway.common;

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
    private String inventoryServiceName;
    private String orderServiceName;
    private String inventoryServiceUrl;
    private String orderServiceUrl;
    private String keycloakUrl;
    private String keycloakRealm;
    private String indexPage;
    private String orderingUiUrl;
    private String orderingUiConfigServiceUrl;
    private String keycloakHostName;
    private String apiHostName;
    private String orderingUiHostName;
    private boolean securityDisabled;
    private boolean rateLimiterDisabled;
    private String rateLimiterCacheServers;
    private String rateLimiterRulesSyncFrequencySeconds;
    private String rateLimiterNumOfVirtualNodes;
    private String rateLimiterRulesTableName;

    public void print() {
        log.info("AppSettings: inventoryServiceName={}", inventoryServiceName);
        log.info("AppSettings: orderServiceName={}", orderServiceName);
        log.info("AppSettings: inventoryServiceUrl={}", inventoryServiceUrl);
        log.info("AppSettings: orderServiceUrl={}", orderServiceUrl);
        log.info("AppSettings: orderingUiConfigServiceUrl={}", orderingUiConfigServiceUrl);
        log.info("AppSettings: orderingUiUrl={}", orderingUiUrl);
        log.info("AppSettings: keycloakUrl={}", keycloakUrl);
        log.info("AppSettings: keycloakRealm={}", keycloakRealm);
        log.info("AppSettings: securityDisabled={}", securityDisabled);
        log.info("AppSettings: rateLimiterDisabled={}", rateLimiterDisabled);
        log.info("AppSettings: rateLimiterCacheServers={}", rateLimiterCacheServers);
        log.info("AppSettings: rateLimiterRulesSyncFrequencySeconds={}", rateLimiterRulesSyncFrequencySeconds);
        log.info("AppSettings: rateLimiterNumOfVirtualNodes={}", rateLimiterNumOfVirtualNodes);        
    }
}
