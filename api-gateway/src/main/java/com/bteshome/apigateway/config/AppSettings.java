package com.bteshome.apigateway.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AppSettings {
    private String inventoryServiceName;
    private String orderServiceName;
    private String inventoryServiceUrl;
    private String orderServiceUrl;
    private String productServiceName;
    private boolean securityDisabled;
    private boolean rateLimiterDisabled;
    private String rateLimiterCacheServers;
    private String rateLimiterRulesDbConnectionString;
    private String rateLimiterRulesSyncFrequencySeconds;
    private String rateLimiterNumOfVirtualNodes;
}
